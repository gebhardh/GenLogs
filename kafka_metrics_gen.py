#!/usr/bin/env python3
"""
kafka_metrics_gen.py - Generator für Kafka-Metriken, wie Telegraf sie liefert,
                       mit Versand an VictoriaMetrics (Influx Line Protocol).

Nachgebildet werden die Messgrößen und Feldnamen von:
  * Telegraf inputs.jolokia2_agent mit der offiziellen Vorlage
    plugins/inputs/jolokia2_agent/examples/kafka.conf   (name_prefix "kafka_")
      kafka_controller, kafka_replica_manager, kafka_purgatory, kafka_request,
      kafka_topics, kafka_topic, kafka_partition  (+ kafka_zookeeper mit --zookeeper)
  * derselben Vorlage für die JVM (examples/java.conf)
      java_runtime, java_memory, java_garbage_collector, java_last_garbage_collection,
      java_threading, java_class_loading, java_memory_pool
  * Telegraf inputs.burrow (Consumer-Lag)
      burrow_group, burrow_partition, burrow_topic

Tags wie bei Telegraf: host, jolokia_agent_url (bzw. cluster/group/topic/partition
bei Burrow). VictoriaMetrics speichert jede Kombination als <messgröße>_<feld>,
z.B. kafka_topics_BytesInPerSec.Count oder burrow_group_total_lag.

Simuliert wird ein KRaft-Cluster (Kafka 3.x/4.x, kombinierte Broker/Controller)
mit Tagesgang und gelegentlichen Störfällen:
  * Consumer-Gruppe wird langsam / steht still  -> Lag steigt, Burrow WARN/ERR
  * Follower hängt hinterher                     -> UnderReplicatedPartitions, ISR-Shrinks
  * Broker-Neustart                              -> Lücke in den Daten, Zähler-Reset,
                                                    Leader-Wechsel, Controller-Wahl
  * Lastspitze auf einem Topic

Nur Python-Standardbibliothek (Python >= 3.9).

Beispiele:
  ./kafka_metrics_gen.py --url http://victoriametrics:8428
  ./kafka_metrics_gen.py --backfill 120          # 2 Stunden Historie, dann live
  ./kafka_metrics_gen.py --stdout --no-send      # nur Line Protocol ausgeben
"""

from __future__ import annotations

import argparse
import base64
import gzip
import json
import logging
import math
import os
import random
import signal
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

log = logging.getLogger("kafka-metrics-gen")

GIB = 1024 ** 3
MIB = 1024 ** 2
PCTS = [("50thPercentile", 0.5), ("75thPercentile", 0.75), ("95thPercentile", 0.95),
        ("98thPercentile", 0.98), ("99thPercentile", 0.99), ("999thPercentile", 0.999)]


def norm_ppf(p: float) -> float:
    """Quantil der Standardnormalverteilung (Acklam-Näherung)."""
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00, 3.754408661907416e+00]
    if p < 0.02425:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / ((((d[0]*q+d[1])*q+d[2])*q+d[3])*q+1)
    if p > 1 - 0.02425:
        return -norm_ppf(1 - p)
    q = p - 0.5
    r = q * q
    return (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q / (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1)


Z = {name: norm_ppf(p) for name, p in PCTS}

# ---------------------------------------------------------------------------
# Yammer-Metriktypen (so wie Kafka sie per JMX/Jolokia liefert)
# ---------------------------------------------------------------------------


class Meter:
    """Kafka/Yammer Meter: Count + EWMA-Raten (1/5/15 min) + MeanRate."""

    def __init__(self, rate: float, uptime: float, event_type: str = "requests"):
        self.count = float(int(max(rate, 0) * uptime))
        self.m1 = self.m5 = self.m15 = max(rate, 0)
        self.uptime = uptime
        self.event_type = event_type

    def update(self, n: float, dt: float) -> None:
        n = max(n, 0.0)
        self.count += round(n)
        self.uptime += dt
        inst = n / dt
        for attr, minutes in (("m1", 1), ("m5", 5), ("m15", 15)):
            alpha = 1 - math.exp(-dt / (60.0 * minutes))
            setattr(self, attr, getattr(self, attr) + alpha * (inst - getattr(self, attr)))

    def fields(self, prefix: str, strings: bool) -> dict:
        f = {f"{prefix}.Count": self.count,
             f"{prefix}.MeanRate": self.count / self.uptime if self.uptime > 0 else 0.0,
             f"{prefix}.OneMinuteRate": self.m1,
             f"{prefix}.FiveMinuteRate": self.m5,
             f"{prefix}.FifteenMinuteRate": self.m15}
        if strings:
            f[f"{prefix}.EventType"] = self.event_type
            f[f"{prefix}.RateUnit"] = "SECONDS"
        return f


class Histogram:
    """Kafka/Yammer Histogram (exponentiell gewichtetes Reservoir, ca. letzte 5 min)."""

    def __init__(self, rnd: random.Random, median: float, sigma: float, rate: float, uptime: float,
                 floor: float = 0.0):
        self.rnd = rnd
        self.count = float(int(max(rate, 0) * uptime))
        self.median, self.sigma, self.floor = median, sigma, floor

    def update(self, n: float, median: float, sigma: float | None = None) -> None:
        self.count += round(max(n, 0))
        # Reservoir reagiert verzögert -> Mittelwert gleitend nachführen
        self.median += 0.35 * (median - self.median)
        if sigma is not None:
            self.sigma += 0.35 * (sigma - self.sigma)

    def fields(self, prefix: str) -> dict:
        r = self.rnd
        m = max(self.median * r.uniform(0.95, 1.05), 1e-9)
        s = self.sigma
        f = {f"{prefix}.Count": self.count}
        if self.count == 0:
            for k in ("Min", "Max", "Mean", "StdDev") + tuple(n for n, _ in PCTS):
                f[f"{prefix}.{k}"] = 0.0
            return f
        mean = m * math.exp(s * s / 2)
        f[f"{prefix}.Mean"] = max(self.floor, mean)
        f[f"{prefix}.StdDev"] = mean * math.sqrt(math.exp(s * s) - 1)
        f[f"{prefix}.Min"] = max(self.floor, m * math.exp(s * -3.1) * r.uniform(0.7, 1.0))
        f[f"{prefix}.Max"] = max(self.floor, m * math.exp(s * 3.4) * r.uniform(1.0, 1.6))
        for name, _ in PCTS:
            f[f"{prefix}.{name}"] = max(self.floor, m * math.exp(s * Z[name]))
        return f


class Timer(Histogram):
    """Yammer Timer = Histogram + Meter-Raten."""

    def __init__(self, rnd, median, sigma, rate, uptime):
        super().__init__(rnd, median, sigma, rate, uptime)
        self.meter = Meter(rate, uptime, "calls")

    def update(self, n, median, sigma=None, dt=10.0):
        super().update(n, median, sigma)
        self.meter.update(n, dt)
        self.count = self.meter.count

    def fields(self, prefix, strings=False):
        f = super().fields(prefix)
        f.update(self.meter.fields(prefix, strings))
        if strings:
            f[f"{prefix}.LatencyUnit"] = "MILLISECONDS"
        return f


# ---------------------------------------------------------------------------
# Cluster-Modell
# ---------------------------------------------------------------------------

DEFAULT_TOPICS = [
    # name, Partitionen, Nachrichten/s (Spitze), mittlere Größe (Byte), Aufbewahrung (h), Consumer-Gruppen
    ("orders", 12, 180, 1400, 168, ["order-service", "analytics"]),
    ("payments", 6, 60, 900, 168, ["payment-service", "fraud-detection"]),
    ("inventory", 6, 90, 600, 72, ["inventory-sync"]),
    ("clickstream", 24, 1500, 450, 24, ["analytics", "recommendation-engine"]),
    ("user-events", 12, 300, 700, 168, ["notification-service", "analytics"]),
    ("audit-log", 3, 25, 2000, 720, ["audit-archiver"]),
]
PRODUCE_REQUESTS = ["Produce", "FetchConsumer", "FetchFollower", "Metadata", "OffsetCommit",
                    "OffsetFetch", "Heartbeat", "JoinGroup", "SyncGroup", "ListOffsets",
                    "FindCoordinator", "ApiVersions"]
REQUEST_HISTOGRAMS = ["TotalTimeMs", "RequestQueueTimeMs", "LocalTimeMs", "RemoteTimeMs",
                      "ResponseQueueTimeMs", "ResponseSendTimeMs"]
PURGATORY_OPS = ["Produce", "Fetch", "DeleteRecords", "ElectLeader", "Heartbeat", "Rebalance", "topic"]
COMPRESSION = 0.45            # Anteil nach Kompression auf der Platte
SEVERITY = {"OK": 0, "WARN": 1, "STALL": 2, "STOP": 3}   # Burrow: schlechtester Partitionsstatus
SEGMENT_BYTES = 1 * GIB


class Partition:
    def __init__(self, rnd, topic, idx, replicas, rate, size, retention_h, age):
        self.topic, self.idx, self.replicas = topic, idx, replicas
        self.preferred = replicas[0]
        self.leader = replicas[0]
        self.weight = rnd.uniform(0.6, 1.4)                  # heiße/kalte Partitionen
        self.avg_rate = rate * 0.6                            # Mittel über den Tag
        self.size = size
        self.retention_msgs = self.avg_rate * retention_h * 3600
        self.seg_msgs = max(1.0, SEGMENT_BYTES / (size * COMPRESSION))
        self.leo = float(int(self.avg_rate * age * rnd.uniform(0.8, 1.2)))
        self.lso = max(0.0, self.leo - self.retention_msgs - rnd.uniform(0, self.seg_msgs))
        self.isr = set(replicas)
        self.follower_lag = {b: 0.0 for b in replicas}        # Nachrichten Rückstand je Replik

    def produce(self, n):
        self.leo += n
        if self.leo - self.lso > self.retention_msgs + self.seg_msgs:
            self.lso += self.seg_msgs                         # ältestes Segment gelöscht

    def log_fields(self, broker):
        leo = self.leo - self.follower_lag.get(broker, 0.0)
        msgs = max(0.0, leo - self.lso)
        size = msgs * self.size * COMPRESSION
        return {"LogEndOffset": float(int(leo)), "LogStartOffset": float(int(self.lso)),
                "Size": float(int(size)), "NumLogSegments": float(max(1, math.ceil(size / SEGMENT_BYTES)))}


class GroupPartition:
    def __init__(self, rnd, part: Partition, group_idx, member):
        self.part = part
        self.committed = part.leo - rnd.uniform(0, part.avg_rate * part.weight * 5)
        self.commit_ts = 0.0
        self.owner = f"/10.20.{group_idx}.{11 + member}"
        self.lag_hist = []


class ConsumerGroup:
    def __init__(self, name, idx):
        self.name, self.idx = name, idx
        self.parts: list[GroupPartition] = []
        self.factor = 1.0              # 1 = normal, <1 langsam, 0 = gestoppt
        self.catchup = False
        self.members = 1


class Broker:
    def __init__(self, rnd, bid, host, args, uptime):
        self.rnd, self.id, self.host = rnd, bid, host
        self.url = f"http://{host}:{args.jolokia_port}/jolokia"
        self.online = True
        self.down_until = 0.0
        self.reset(uptime, args)

    def reset(self, uptime, args):
        """(Neu-)Start der JVM: alle Zähler beginnen bei den Werten nach 'uptime' Sekunden."""
        r = self.rnd
        self.uptime = uptime
        self.topic_meters: dict[str, dict[str, Meter]] = {}
        self.agg_meters: dict[str, Meter] = {}
        self.req_hist = {}
        self.req_rates = {}
        self.rm_meters = {n: Meter(0.0, uptime, "expands" if "Expand" in n else "shrinks")
                          for n in ("IsrExpandsPerSec", "IsrShrinksPerSec", "FailedIsrUpdatesPerSec")}
        self.unclean = Meter(0.0, uptime, "elections")
        self.elections = Timer(r, 25.0, 0.5, 0.0, uptime)
        self.zk = {n: Meter(0.0, uptime, "events") for n in
                   ("ZooKeeperAuthFailuresPerSec", "ZooKeeperDisconnectsPerSec", "ZooKeeperExpiresPerSec",
                    "ZooKeeperReadOnlyConnectsPerSec", "ZooKeeperSaslAuthenticationsPerSec",
                    "ZooKeeperSyncConnectsPerSec")}
        # JVM (G1, -Xmx6g)
        self.heap_max = args.heap_gb * GIB
        self.eden_cap = self.heap_max * 0.30
        self.eden = r.uniform(0.1, 0.9) * self.eden_cap
        self.survivor = 40 * MIB
        self.old = self.heap_max * r.uniform(0.18, 0.30)
        self.peak = {"G1 Eden Space": self.eden_cap, "G1 Survivor Space": 80 * MIB, "G1 Old Gen": self.old}
        self.gc = {"G1 Young Generation": [uptime / 25.0, uptime / 25.0 * 14],     # [count, time ms]
                   "G1 Old Generation": [0.0, 0.0],
                   "G1 Concurrent GC": [uptime / 3600.0, uptime / 3600.0 * 9]}
        self.last_gc = (15.0, 4.0)
        self.threads_started = 180 + uptime / 600
        self.metaspace = 62 * MIB + r.uniform(0, 8 * MIB)


class Cluster:
    def __init__(self, args, now):
        self.args = args
        self.rnd = rnd = random.Random(args.seed)
        n = args.brokers
        self.rf = min(3, n)
        self.min_isr = min(2, self.rf)
        # Ein Start des Generators entspricht einem Start der Broker: JMX-Zähler beginnen
        # bei 0 (rate() erkennt das als Zähler-Reset). Die Offsets überleben dagegen über
        # die Statusdatei, wie die Daten eines echten Kafka-Clusters.
        self.brokers = [Broker(rnd, i + 1, f"{args.host_prefix}{i + 1}", args, rnd.uniform(5, 40))
                        for i in range(n)]
        self.data_age = rnd.uniform(30, 400) * 86400          # Alter des Clusters für Start-Offsets
        self.topics = self._parse_topics(args.topics)
        self.parts: list[Partition] = []
        self.topic_parts: dict[str, list[Partition]] = {}
        for t_i, (name, np_, rate, size, ret_h, _groups) in enumerate(self.topics):
            lst = []
            for p in range(np_):
                reps = [self.brokers[(p + t_i + k) % n].id for k in range(self.rf)]
                part = Partition(rnd, name, p, reps, rate / np_, size, ret_h, self.data_age)
                lst.append(part)
            self.topic_parts[name] = lst
            self.parts += lst
        # interne Offsets-Topic
        co = []
        for p in range(50):
            reps = [self.brokers[(p + k) % n].id for k in range(self.rf)]
            co.append(Partition(rnd, "__consumer_offsets", p, reps, 2.0 / 50, 120, 168, self.data_age))
        self.topic_parts["__consumer_offsets"] = co
        self.parts += co
        # Consumer-Gruppen
        self.groups: dict[str, ConsumerGroup] = {}
        for name, *_x, groups in self.topics:
            for g in groups:
                grp = self.groups.setdefault(g, ConsumerGroup(g, len(self.groups) + 1))
                grp.members = max(grp.members, 2 if len(self.topic_parts[name]) < 12 else 4)
                for p in self.topic_parts[name]:
                    grp.parts.append(GroupPartition(rnd, p, grp.idx, p.idx % grp.members))
        for grp in self.groups.values():
            for gp in grp.parts:
                gp.commit_ts = now
        self.controller = self.brokers[0].id
        # Störfälle
        self.next_incident = now + rnd.uniform(240, 900)
        self.incident = None            # (typ, ende, daten)
        self.spike = {}                 # topic -> faktor
        self.lagging_broker = None
        self.imbalance_until = 0.0

    @staticmethod
    def _parse_topics(spec):
        if not spec:
            return DEFAULT_TOPICS
        out = []
        for item in spec.split(","):
            name, _, rest = item.strip().partition(":")
            parts = int(rest or 6)
            out.append((name, parts, 50.0 * parts, 800, 168, [f"{name}-consumer"]))
        return out

    def broker(self, bid) -> Broker:
        return self.brokers[bid - 1]

    # -- Statusdatei: Offsets überleben Neustarts -------------------------------
    def save_state(self, path, now):
        state = {"version": 1, "saved": now,
                 "topics": {t: [[p.leo, p.lso] for p in ps] for t, ps in self.topic_parts.items()},
                 "groups": {g.name: {f"{gp.part.topic}/{gp.part.idx}": gp.committed for gp in g.parts}
                            for g in self.groups.values()}}
        tmp = path + ".tmp"
        try:
            with open(tmp, "w") as f:
                json.dump(state, f)
            os.replace(tmp, path)
        except OSError as e:
            log.warning("Statusdatei %s nicht schreibbar: %s", path, e)

    def load_state(self, path):
        try:
            with open(path) as f:
                state = json.load(f)
        except FileNotFoundError:
            return False
        except (OSError, ValueError) as e:
            log.warning("Statusdatei %s nicht lesbar (%s) - beginne neu.", path, e)
            return False
        restored = 0
        for t, ps in self.topic_parts.items():
            saved = state.get("topics", {}).get(t)
            if saved and len(saved) == len(ps):          # nur bei gleicher Partitionsanzahl
                for p, (leo, lso) in zip(ps, saved):
                    p.leo, p.lso = float(leo), float(lso)
                restored += 1
        for g in self.groups.values():
            saved = state.get("groups", {}).get(g.name, {})
            for gp in g.parts:
                c = saved.get(f"{gp.part.topic}/{gp.part.idx}")
                if c is not None:
                    gp.committed = min(float(c), gp.part.leo)
        log.info("Offsets aus %s übernommen (%d von %d Topics, Stand %s).", path, restored,
                 len(self.topic_parts), time.strftime("%d.%m. %H:%M", time.localtime(state.get("saved", 0))))
        return True

    # -- Tagesgang ----------------------------------------------------------
    @staticmethod
    def diurnal(ts):
        lt = time.localtime(ts)
        h = lt.tm_hour + lt.tm_min / 60
        base = 0.55 + 0.45 * math.cos((h - 14) / 24 * 2 * math.pi)      # Spitze 14 Uhr
        if lt.tm_wday >= 5:
            base *= 0.7
        return max(0.12, base)

    # -- Störfälle ------------------------------------------------------------
    def update_incidents(self, now):
        r = self.rnd
        if self.incident and now >= self.incident[1]:
            self._end_incident(now)
        if self.incident is None and now >= self.next_incident:
            kind = r.choices(["consumer_slow", "consumer_stop", "follower_lag", "broker_restart", "spike"],
                             [0.28, 0.17, 0.2, 0.13, 0.22])[0]
            dur = {"consumer_slow": (300, 900), "consumer_stop": (120, 420), "follower_lag": (120, 600),
                   "broker_restart": (60, 180), "spike": (180, 600)}[kind]
            end = now + r.uniform(*dur)
            data = None
            if kind in ("consumer_slow", "consumer_stop"):
                grp = r.choice(list(self.groups.values()))
                grp.factor = r.uniform(0.25, 0.6) if kind == "consumer_slow" else 0.0
                data = grp.name
            elif kind == "follower_lag":
                self.lagging_broker = r.choice(self.brokers).id
                data = self.lagging_broker
                self._shrink_isr(self.lagging_broker)
            elif kind == "broker_restart":
                b = r.choice(self.brokers)
                data = b.id
                self._broker_down(b, now)
            elif kind == "spike":
                t = r.choice(self.topics)[0]
                self.spike[t] = r.uniform(2.0, 4.0)
                data = t
            self.incident = (kind, end, data)
            log.info("[%s] Störfall: %s (%s) für %.0f s", time.strftime("%d.%m. %H:%M:%S", time.localtime(now)), kind, data, end - now)

    def _end_incident(self, now):
        kind, _end, data = self.incident
        r = self.rnd
        if kind in ("consumer_slow", "consumer_stop"):
            grp = self.groups[data]
            grp.factor, grp.catchup = 1.0, True
        elif kind == "follower_lag":
            self._expand_isr(data)
            self.lagging_broker = None
        elif kind == "broker_restart":
            b = self.broker(data)
            b.online = True
            b.reset(0.0, self.args)
            for p in self.parts:                      # Follower holt auf
                if data in p.replicas:
                    p.follower_lag[data] = max(p.follower_lag[data], p.avg_rate * p.weight * 30)
            self.imbalance_until = now + 300           # bevorzugte Leader nach 5 min zurück
        elif kind == "spike":
            self.spike.pop(data, None)
        log.info("[%s] Störfall beendet: %s (%s)", time.strftime("%d.%m. %H:%M:%S", time.localtime(now)), kind, data)
        self.incident = None
        self.next_incident = now + r.uniform(600, 2400)

    def _shrink_isr(self, bid):
        for p in self.parts:
            if bid in p.replicas and p.leader != bid and bid in p.isr:
                p.isr.discard(bid)
                self.broker(p.leader).rm_meters["IsrShrinksPerSec"].count += 1

    def _expand_isr(self, bid):
        for p in self.parts:
            if bid in p.replicas and bid not in p.isr and self.broker(bid).online:
                p.isr.add(bid)
                p.follower_lag[bid] = 0.0
                self.broker(p.leader).rm_meters["IsrExpandsPerSec"].count += 1

    def _broker_down(self, b: Broker, now):
        b.online = False
        moved = 0
        for p in self.parts:
            if b.id in p.replicas:
                p.isr.discard(b.id)
                if p.leader == b.id:
                    candidates = [x for x in p.replicas if x != b.id and x in p.isr]
                    if candidates:
                        p.leader = candidates[0]
                        moved += 1
                        self.broker(p.leader).rm_meters["IsrShrinksPerSec"].count += 1
        if self.controller == b.id:
            self.controller = next(x.id for x in self.brokers if x.online)
        ctrl = self.broker(self.controller)
        ctrl.elections.update(moved, 30.0, 0.4, dt=self.args.interval)

    # -- ein Messintervall --------------------------------------------------------
    def step(self, now, dt):
        r = self.rnd
        self.update_incidents(now)
        load = self.diurnal(now)

        # bevorzugte Leader zurück (auto.leader.rebalance)
        if self.imbalance_until and now >= self.imbalance_until:
            moved = 0
            for p in self.parts:
                if p.leader != p.preferred and self.broker(p.preferred).online and p.preferred in p.isr:
                    p.leader = p.preferred
                    moved += 1
            self.broker(self.controller).elections.update(moved, 20.0, 0.4, dt=dt)
            self.imbalance_until = 0.0

        # Follower holen auf / fallen zurück
        for p in self.parts:
            for b in p.replicas:
                if b == p.leader:
                    p.follower_lag[b] = 0.0
                elif b == self.lagging_broker or not self.broker(b).online:
                    p.follower_lag[b] += p.avg_rate * p.weight * load * dt * 0.7
                elif p.follower_lag[b] > 0:
                    p.follower_lag[b] = max(0.0, p.follower_lag[b] - p.avg_rate * p.weight * dt * 4)
                    if p.follower_lag[b] == 0 and b not in p.isr:
                        p.isr.add(b)
                        self.broker(p.leader).rm_meters["IsrExpandsPerSec"].count += 1

        # Produktion je Partition
        prod: dict[int, dict[str, float]] = {b.id: {} for b in self.brokers}       # leader -> topic -> msgs
        bytes_in: dict[int, dict[str, float]] = {b.id: {} for b in self.brokers}
        for name, np_, rate, size, _ret, _g in self.topics:
            f = load * self.spike.get(name, 1.0)
            for p in self.topic_parts[name]:
                n = max(0.0, r.gauss(rate / np_ * p.weight * f * dt, math.sqrt(rate / np_ * dt + 1)))
                p.produce(n)
                prod[p.leader][name] = prod[p.leader].get(name, 0.0) + n
                bytes_in[p.leader][name] = bytes_in[p.leader].get(name, 0.0) + n * size * r.uniform(0.9, 1.1)

        # Konsum je Gruppe
        consumed: dict[int, dict[str, float]] = {b.id: {} for b in self.brokers}
        commits = 0.0
        for grp in self.groups.values():
            behind = False
            for gp in grp.parts:
                p = gp.part
                avail = p.leo - gp.committed
                cap = p.avg_rate * p.weight * dt * (2.5 if grp.catchup else 1.6) * grp.factor
                take = min(avail, cap) if grp.factor > 0 else 0.0
                # nicht ganz bis zum Ende (Commit-Intervall)
                take = max(0.0, take - r.uniform(0, p.avg_rate * p.weight * load * 2))
                if take > 0:
                    gp.committed += take
                    gp.commit_ts = now
                    commits += dt / 5.0
                    consumed[p.leader][p.topic] = consumed[p.leader].get(p.topic, 0.0) + take
                lag = p.leo - gp.committed
                gp.lag_hist = (gp.lag_hist + [lag])[-10:]
                if lag > p.avg_rate * p.weight * 60:
                    behind = True
            if grp.catchup and not behind:
                grp.catchup = False

        co = self.topic_parts["__consumer_offsets"]
        for p in co:
            n = commits / len(co) * r.uniform(0.6, 1.4)
            p.produce(n)
            prod[p.leader]["__consumer_offsets"] = prod[p.leader].get("__consumer_offsets", 0.0) + n
            bytes_in[p.leader]["__consumer_offsets"] = bytes_in[p.leader].get("__consumer_offsets", 0.0) + n * 120

        for b in self.brokers:
            if b.online:
                self._broker_step(b, dt, load, prod[b.id], bytes_in[b.id], consumed[b.id])

    def _broker_step(self, b: Broker, dt, load, prod, bin_, cons):
        r = self.rnd
        a = self.args
        b.uptime += dt
        sizes = {t[0]: t[3] for t in self.topics}
        sizes["__consumer_offsets"] = 120
        bad = self.lagging_broker is not None or any(not x.online for x in self.brokers)
        # Replikation: Follower bekommen Bytes der Partitionen, deren Leader andere sind
        repl_in = sum(p.avg_rate * p.weight * load * dt * p.size for p in self.parts
                      if b.id in p.replicas and p.leader != b.id and self.lagging_broker != b.id) \
            * r.uniform(0.9, 1.1)
        led = [p for p in self.parts if p.leader == b.id]
        repl_out = sum(p.avg_rate * p.weight * load * dt * p.size * (len(p.isr) - 1) for p in led)

        # BrokerTopicMetrics je Topic
        tot = {k: 0.0 for k in ("MessagesInPerSec", "BytesInPerSec", "BytesOutPerSec", "TotalProduceRequestsPerSec",
                                "TotalFetchRequestsPerSec", "FailedProduceRequestsPerSec",
                                "FailedFetchRequestsPerSec", "BytesRejectedPerSec",
                                "ProduceMessageConversionsPerSec", "FetchMessageConversionsPerSec")}
        topic_names = set(prod) | set(cons) | {p.topic for p in self.parts if b.id in p.replicas}
        for t in topic_names:
            msgs, bi = prod.get(t, 0.0), bin_.get(t, 0.0)
            bo = cons.get(t, 0.0) * sizes.get(t, 500)
            vals = {"MessagesInPerSec": msgs, "BytesInPerSec": bi, "BytesOutPerSec": bo,
                    "TotalProduceRequestsPerSec": msgs / r.uniform(25, 60) + (dt * 0.5 if msgs else 0),
                    "TotalFetchRequestsPerSec": dt * 2.0 * (1 + len([p for p in led if p.topic == t]) * 0.2),
                    "FailedProduceRequestsPerSec": (r.random() < (0.05 if bad else 0.003)) * r.randint(1, 3),
                    "FailedFetchRequestsPerSec": (r.random() < (0.05 if bad else 0.002)) * 1,
                    "BytesRejectedPerSec": 0.0, "ProduceMessageConversionsPerSec": 0.0,
                    "FetchMessageConversionsPerSec": 0.0}
            meters = b.topic_meters.setdefault(t, {})
            for k, v in vals.items():
                if k not in meters:
                    meters[k] = Meter(v / dt, b.uptime * 0.98, "bytes" if "Bytes" in k else
                                      ("messages" if "Messages" in k or "Conversions" in k else "requests"))
                meters[k].update(v, dt)
                tot[k] += v
        tot["ReplicationBytesInPerSec"] = repl_in
        tot["ReplicationBytesOutPerSec"] = repl_out
        tot["ReassignmentBytesInPerSec"] = 0.0
        tot["ReassignmentBytesOutPerSec"] = 0.0
        tot["NoKeyCompactedTopicRecordsPerSec"] = 0.0
        tot["InvalidMessageCrcRecordsPerSec"] = 0.0
        tot["InvalidMagicNumberRecordsPerSec"] = 0.0
        for k, v in tot.items():
            if k not in b.agg_meters:
                b.agg_meters[k] = Meter(v / dt, b.uptime * 0.98, "bytes" if "Bytes" in k else "messages")
            b.agg_meters[k].update(v, dt)

        # RequestMetrics
        consumers = sum(g.members for g in self.groups.values())
        lat_f = 1.0 + (1.8 if self.lagging_broker is not None else 0.0)
        msgs_in = tot["MessagesInPerSec"] / dt
        req = {   # Requests/s, (Median, Sigma) TotalTimeMs
            "Produce": (max(1.0, msgs_in / 40), 3.0 * lat_f * (0.8 + 0.4 * load), 0.55),
            "FetchConsumer": (consumers * 2.0 / len(self.brokers), 480.0, 0.08),
            "FetchFollower": ((self.rf - 1) * 2.0 * (len(self.brokers) - 1) / max(1, len(self.brokers) - 1), 495.0, 0.04),
            "Metadata": (0.4, 0.6, 0.5), "OffsetCommit": (consumers / 5.0 / len(self.brokers), 2.5, 0.5),
            "OffsetFetch": (0.05, 1.2, 0.5), "Heartbeat": (consumers / 3.0 / len(self.brokers), 0.3, 0.4),
            "JoinGroup": (0.002, 3000.0, 0.6), "SyncGroup": (0.002, 6.0, 0.6), "ListOffsets": (0.1, 0.8, 0.5),
            "FindCoordinator": (0.01, 0.5, 0.4), "ApiVersions": (0.05, 0.2, 0.3)}
        for name, (rps, med, sig) in req.items():
            n = max(0.0, r.gauss(rps * dt, math.sqrt(rps * dt) + 0.01))
            parts = {
                "TotalTimeMs": (med, sig),
                "RequestQueueTimeMs": (0.05 + 0.02 * load * lat_f, 0.8),
                "LocalTimeMs": ({"Produce": 0.9, "FetchConsumer": 0.6, "FetchFollower": 0.5}.get(name, med * 0.6), 0.6),
                "RemoteTimeMs": ((med * 0.75) if name in ("Produce", "FetchConsumer", "FetchFollower") else 0.001, sig),
                "ResponseQueueTimeMs": (0.03, 0.7),
                "ResponseSendTimeMs": (0.05 + (0.15 if name.startswith("Fetch") else 0.0), 0.7),
            }
            hs = b.req_hist.setdefault(name, {})
            for hn, (m, s) in parts.items():
                if hn not in hs:
                    hs[hn] = Histogram(r, m, s, rps, b.uptime)
                hs[hn].update(n, m, s)

        for m in b.rm_meters.values():
            m.update(0, dt)          # Zähler wurden ggf. direkt erhöht
        b.unclean.update(0, dt)
        b.elections.update(0, b.elections.median, dt=dt)
        for m in b.zk.values():
            m.update(0, dt)

        # JVM: Allokation proportional zum Datenverkehr
        alloc = (tot["BytesInPerSec"] + tot["BytesOutPerSec"] + repl_in + repl_out) * 0.6 + 25 * MIB * dt
        b.eden += alloc
        young = 0
        while b.eden >= b.eden_cap:
            b.eden -= b.eden_cap
            young += 1
            promoted = r.uniform(2, 12) * MIB
            b.old += promoted
            b.survivor = r.uniform(20, 90) * MIB
        if young:
            dur = r.uniform(6, 22) * (1 + 0.3 * load)
            b.gc["G1 Young Generation"][0] += young
            b.gc["G1 Young Generation"][1] += young * dur
            b.last_gc = (dur, float(a.gc_threads))
        if b.old > b.heap_max * 0.45:              # IHOP erreicht -> Concurrent Cycle + Mixed GCs
            b.gc["G1 Concurrent GC"][0] += 1
            b.gc["G1 Concurrent GC"][1] += r.uniform(5, 20)
            freed = b.old * r.uniform(0.35, 0.55)
            b.old -= freed
            b.gc["G1 Young Generation"][0] += 3
            b.gc["G1 Young Generation"][1] += 3 * r.uniform(15, 40)
        b.peak["G1 Old Gen"] = max(b.peak["G1 Old Gen"], b.old)
        b.peak["G1 Survivor Space"] = max(b.peak["G1 Survivor Space"], b.survivor)
        b.threads_started += r.random() < 0.05
        b.metaspace = min(b.metaspace + r.uniform(0, 2000), 90 * MIB)

    # -- Ausgabe ----------------------------------------------------------------
    def points(self, now):
        """Liefert (Messgröße, Tags, Felder) wie Telegraf sie schreiben würde."""
        a = self.args
        s = a.string_fields
        out = []
        for b in self.brokers:
            if not b.online:
                continue                        # Jolokia nicht erreichbar -> keine Daten
            base = {"host": b.host, "jolokia_agent_url": b.url}
            if not a.no_kafka:
                out += self._kafka_points(b, base, s)
            if not a.no_java:
                out += self._java_points(b, base)
        if not a.no_burrow:
            out += self._burrow_points(now, s)
        if a.tag:
            extra = dict(t.split("=", 1) for t in a.tag)
            out = [(m, {**extra, **tags}, f) for m, tags, f in out]
        return out

    def _kafka_points(self, b: Broker, base, s):
        out = []
        led = [p for p in self.parts if p.leader == b.id]
        hosted = [p for p in self.parts if b.id in p.replicas]
        online = [x for x in self.brokers if x.online]
        # kafka_controller (KafkaController + ControllerStats, gleiche Tags -> ein Punkt)
        active = self.controller == b.id
        f = {"ActiveControllerCount.Value": float(active),
             "OfflinePartitionsCount.Value": 0.0,
             "PreferredReplicaImbalanceCount.Value": float(sum(p.leader != p.preferred for p in self.parts))
             if active else 0.0,
             "GlobalTopicCount.Value": float(len(self.topic_parts)) if active else 0.0,
             "GlobalPartitionCount.Value": float(len(self.parts)) if active else 0.0,
             "ActiveBrokerCount.Value": float(len(online)) if active else 0.0,
             "FencedBrokerCount.Value": float(len(self.brokers) - len(online)) if active else 0.0,
             "TopicsToDeleteCount.Value": 0.0, "ReplicasToDeleteCount.Value": 0.0}
        f.update(b.elections.fields("LeaderElectionRateAndTimeMs", s))
        f.update(b.unclean.fields("UncleanLeaderElectionsPerSec", s))
        out.append(("kafka_controller", dict(base), f))
        # kafka_replica_manager
        urp = [p for p in led if len(p.isr) < len(p.replicas)]
        f = {"LeaderCount.Value": float(len(led)), "PartitionCount.Value": float(len(hosted)),
             "UnderReplicatedPartitions.Value": float(len(urp)),
             "UnderMinIsrPartitionCount.Value": float(sum(len(p.isr) < self.min_isr for p in led)),
             "AtMinIsrPartitionCount.Value": float(sum(len(p.isr) == self.min_isr for p in led)),
             "OfflineReplicaCount.Value": 0.0, "ReassigningPartitions.Value": 0.0}
        for n, m in b.rm_meters.items():
            f.update(m.fields(n, s))
        out.append(("kafka_replica_manager", dict(base), f))
        # kafka_purgatory
        f = {}
        cons = sum(g.members for g in self.groups.values())
        for op in PURGATORY_OPS:
            size = {"Produce": self.rnd.randint(0, 6), "Fetch": cons + self.rf * 2 + self.rnd.randint(0, 5)}.get(op, 0)
            f[f"{op}.PurgatorySize"] = float(size)
            f[f"{op}.NumDelayedOperations"] = float(size)
        out.append(("kafka_purgatory", dict(base), f))
        # kafka_zookeeper (nur ZooKeeper-Modus)
        if self.args.zookeeper:
            f = {}
            for n, m in b.zk.items():
                f.update(m.fields(n, s))
            out.append(("kafka_zookeeper", dict(base), f))
        # kafka_request (tag request)
        for req, hs in b.req_hist.items():
            f = {}
            for hn, h in hs.items():
                f.update(h.fields(hn))
            out.append(("kafka_request", {**base, "request": req}, f))
        # kafka_topics (aggregiert) und kafka_topic (je Topic)
        f = {}
        for n, m in b.agg_meters.items():
            f.update(m.fields(n, s))
        out.append(("kafka_topics", dict(base), f))
        for t, ms in sorted(b.topic_meters.items()):
            f = {}
            for n, m in ms.items():
                f.update(m.fields(n, s))
            out.append(("kafka_topic", {**base, "topic": t}, f))
        # kafka_partition (Log-Metriken + UnderReplicated)
        for p in hosted:
            tags = {**base, "topic": p.topic, "partition": str(p.idx)}
            out.append(("kafka_partition", tags, p.log_fields(b.id)))
            out.append(("kafka_partition", dict(tags),
                        {"UnderReplicatedPartitions": float(p.leader == b.id and len(p.isr) < len(p.replicas))}))
        return out

    def _java_points(self, b: Broker, base):
        r = self.rnd
        heap_used = b.eden + b.survivor + b.old
        heap_committed = b.heap_max
        nonheap_used = b.metaspace + 9 * MIB + 28 * MIB
        out = [
            ("java_runtime", dict(base), {"Uptime": float(int(b.uptime * 1000))}),
            ("java_memory", dict(base), {
                "HeapMemoryUsage.init": float(b.heap_max), "HeapMemoryUsage.used": float(int(heap_used)),
                "HeapMemoryUsage.committed": float(heap_committed), "HeapMemoryUsage.max": float(b.heap_max),
                "NonHeapMemoryUsage.init": float(7667712), "NonHeapMemoryUsage.used": float(int(nonheap_used)),
                "NonHeapMemoryUsage.committed": float(int(nonheap_used * 1.08)), "NonHeapMemoryUsage.max": -1.0,
                "ObjectPendingFinalizationCount": 0.0}),
        ]
        for name, (cnt, tm) in b.gc.items():
            out.append(("java_garbage_collector", {**base, "name": name},
                        {"CollectionCount": float(int(cnt)), "CollectionTime": float(int(tm))}))
        dur, threads = b.last_gc
        out.append(("java_last_garbage_collection", dict(base),
                    {"LastGcInfo.duration": float(int(dur)), "LastGcInfo.GcThreadCount": threads}))
        conns = sum(g.members for g in self.groups.values()) + 20
        tc = 95 + conns // 3 + r.randint(-2, 2)
        out.append(("java_threading", dict(base), {
            "ThreadCount": float(tc), "DaemonThreadCount": float(tc - 12), "PeakThreadCount": float(tc + 9),
            "TotalStartedThreadCount": float(int(b.threads_started))}))
        loaded = 9100 + int(b.metaspace / MIB) * 10
        out.append(("java_class_loading", dict(base), {
            "LoadedClassCount": float(loaded), "UnloadedClassCount": float(int(b.uptime / 7200)),
            "TotalLoadedClassCount": float(loaded + int(b.uptime / 7200))}))

        def usage(prefix, init, used, committed, mx):
            return {f"{prefix}.init": float(int(init)), f"{prefix}.used": float(int(used)),
                    f"{prefix}.committed": float(int(committed)), f"{prefix}.max": float(mx)}
        pools = {
            "G1 Eden Space": (b.eden, b.eden_cap, -1, b.peak["G1 Eden Space"], 0.0),
            "G1 Survivor Space": (b.survivor, b.survivor, -1, b.peak["G1 Survivor Space"], b.survivor),
            "G1 Old Gen": (b.old, b.heap_max - b.eden_cap - b.survivor, b.heap_max, b.peak["G1 Old Gen"],
                           b.old * 0.8),
            "Metaspace": (b.metaspace, b.metaspace * 1.05, -1, b.metaspace, None),
            "Compressed Class Space": (b.metaspace * 0.12, b.metaspace * 0.13, GIB, b.metaspace * 0.12, None),
            "CodeHeap 'non-nmethods'": (2.1 * MIB, 2.5 * MIB, 5.8 * MIB, 2.6 * MIB, None),
            "CodeHeap 'profiled nmethods'": (14 * MIB, 16 * MIB, 117 * MIB, 18 * MIB, None),
            "CodeHeap 'non-profiled nmethods'": (12 * MIB, 13 * MIB, 117 * MIB, 13 * MIB, None),
        }
        for name, (used, committed, mx, peak, coll) in pools.items():
            f = usage("Usage", 0 if "G1" not in name else committed, used, committed, mx)
            f.update(usage("PeakUsage", 0 if "G1" not in name else committed, peak, committed, mx))
            if coll is not None:                   # CollectionUsage nur bei Heap-Pools (sonst null)
                f.update(usage("CollectionUsage", committed, coll, committed, mx))
            out.append(("java_memory_pool", {**base, "name": name}, f))
        return out

    def _burrow_points(self, now, s):
        a = self.args
        base = {"host": a.burrow_host}
        out = []
        codes = {"OK": 1, "NOT_FOUND": 2, "WARN": 3, "ERR": 4, "STOP": 5, "STALL": 6}
        for t, parts in self.topic_parts.items():
            if t.startswith("__"):
                continue
            for p in parts:
                out.append(("burrow_topic", {**base, "cluster": a.cluster, "topic": t, "partition": str(p.idx)},
                            {"offset": int(p.leo)}))
        for grp in self.groups.values():
            worst, total, maxp = "OK", 0, None
            for gp in grp.parts:
                p = gp.part
                lag = max(0, int(p.leo - gp.committed))
                total += lag
                hist = gp.lag_hist
                if grp.factor == 0 and lag > 0:
                    st = "STOP"
                elif len(hist) >= 5 and all(x < y for x, y in zip(hist[-5:], hist[-4:])) and \
                        lag > p.avg_rate * p.weight * 30:
                    st = "WARN"
                else:
                    st = "OK"
                if SEVERITY[st] > SEVERITY[worst]:
                    worst = st
                if maxp is None or lag > maxp[0]:
                    maxp = (lag, gp)
                f = {"lag": lag, "offset": int(gp.committed), "timestamp": int(gp.commit_ts * 1000),
                     "status_code": codes[st]}
                if s:
                    f["status"] = st
                out.append(("burrow_partition", {**base, "cluster": a.cluster, "group": grp.name, "topic": p.topic,
                                                 "partition": str(p.idx),
                                                 "owner": gp.owner if grp.factor > 0 else ""}, f))
            gstatus = "ERR" if worst in ("STOP", "STALL") else worst
            lag, gp = maxp
            f = {"status_code": codes[gstatus], "partition_count": len(grp.parts), "total_lag": total,
                 "lag": lag, "offset": int(gp.committed), "timestamp": int(gp.commit_ts * 1000)}
            if s:
                f["status"] = gstatus
            out.append(("burrow_group", {**base, "cluster": a.cluster, "group": grp.name}, f))
        return out


# ---------------------------------------------------------------------------
# Influx Line Protocol + Versand
# ---------------------------------------------------------------------------

def _esc(s: str, chars=",= ") -> str:
    s = s.replace("\\", "\\\\")
    for c in chars:
        s = s.replace(c, "\\" + c)
    return s


def to_line(measurement: str, tags: dict, fields: dict, ts_ns: int) -> str | None:
    fs = []
    for k, v in fields.items():
        if v is None:
            continue
        if isinstance(v, bool):
            val = "true" if v else "false"
        elif isinstance(v, int):
            val = f"{v}i"                                  # Burrow: Ganzzahlen wie bei Telegraf
        elif isinstance(v, float):
            if math.isnan(v) or math.isinf(v):
                continue
            val = repr(v)                                  # Jolokia-Werte sind bei Telegraf float64
        else:
            val = '"' + str(v).replace("\\", "\\\\").replace('"', '\\"') + '"'
        fs.append(f"{_esc(k)}={val}")
    if not fs:
        return None
    tag_str = "".join(f",{_esc(k)}={_esc(str(v))}" for k, v in sorted(tags.items()) if v != "")
    return f"{_esc(measurement, ', ')}{tag_str} {','.join(fs)} {ts_ns}"


class VMSender:
    def __init__(self, url, path, user, password, bearer, timeout, max_buffer):
        self.url = url.rstrip("/") + path
        self.timeout = timeout
        self.max_buffer = max_buffer
        self.headers = {"Content-Type": "text/plain; charset=utf-8", "Content-Encoding": "gzip"}
        if bearer:
            self.headers["Authorization"] = "Bearer " + bearer
        elif user:
            self.headers["Authorization"] = "Basic " + base64.b64encode(f"{user}:{password or ''}".encode()).decode()
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self.buffer: list[str] = []
        self.sent = self.dropped = 0
        self.failed_since = None

    def send(self, lines):
        self.buffer.extend(lines)
        if len(self.buffer) > self.max_buffer:
            drop = len(self.buffer) - self.max_buffer
            del self.buffer[:drop]
            self.dropped += drop
        while self.buffer:
            chunk = self.buffer[:50_000]
            body = gzip.compress(("\n".join(chunk) + "\n").encode("utf-8"), compresslevel=5)
            req = urllib.request.Request(self.url, data=body, method="POST", headers=self.headers)
            try:
                with self.opener.open(req, timeout=self.timeout) as resp:
                    resp.read()
            except urllib.error.HTTPError as e:
                msg = e.read().decode("utf-8", "replace")[:300]
                if e.code in (400, 422):
                    log.error("VictoriaMetrics lehnt Daten ab (HTTP %d): %s - %d Zeilen verworfen",
                              e.code, msg, len(chunk))
                    del self.buffer[:len(chunk)]
                    continue
                return self._fail(f"HTTP {e.code}: {msg}")
            except (urllib.error.URLError, OSError) as e:
                return self._fail(str(e))
            del self.buffer[:len(chunk)]
            self.sent += len(chunk)
            if self.failed_since is not None:
                log.info("Verbindung zu VictoriaMetrics wieder hergestellt.")
                self.failed_since = None
        return True

    def _fail(self, reason):
        if self.failed_since is None:
            self.failed_since = time.time()
            log.warning("Senden fehlgeschlagen (%s) - puffere bis zu %d Zeilen.", reason, self.max_buffer)
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def env(name, default):
    return os.environ.get(name, default)


def env_bool(name):
    return env(name, "").strip().lower() in ("1", "true", "yes", "ja")


def parse_args():
    p = argparse.ArgumentParser(description="Kafka-Metrik-Generator (Telegraf-Format) für VictoriaMetrics",
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--url", default=env("VM_URL", "http://localhost:8428"), help="VictoriaMetrics-URL (env VM_URL)")
    p.add_argument("--path", default=env("VM_WRITE_PATH", "/write"),
                   help="Schreibpfad; Cluster: /insert/<tenant>/influx/write (env VM_WRITE_PATH)")
    p.add_argument("--user", default=env("VM_USER", None), help="Basic-Auth-Benutzer (env VM_USER)")
    p.add_argument("--password", default=env("VM_PASSWORD", None), help="Basic-Auth-Passwort (env VM_PASSWORD)")
    p.add_argument("--bearer-token", default=env("VM_BEARER_TOKEN", None), help="Bearer-Token (env VM_BEARER_TOKEN)")
    p.add_argument("--interval", type=int, default=int(env("KMG_INTERVAL", 10)),
                   help="Erfassungsintervall in s wie Telegraf 'interval' (env KMG_INTERVAL)")
    p.add_argument("--brokers", type=int, default=int(env("KMG_BROKERS", 3)), help="Anzahl Broker (env KMG_BROKERS)")
    p.add_argument("--host-prefix", default=env("KMG_HOST_PREFIX", "kafka-"),
                   help="Hostnamen der Broker: <prefix>1..N (env KMG_HOST_PREFIX)")
    p.add_argument("--jolokia-port", type=int, default=int(env("KMG_JOLOKIA_PORT", 8778)), help="Port in jolokia_agent_url")
    p.add_argument("--cluster", default=env("KMG_CLUSTER", "kafka-prod"), help="Clustername für Burrow (env KMG_CLUSTER)")
    p.add_argument("--burrow-host", default=env("KMG_BURROW_HOST", "burrow"), help="host-Tag der Burrow-Metriken")
    p.add_argument("--topics", default=env("KMG_TOPICS", ""),
                   help="eigene Topics 'name:partitionen,...' statt der Beispiel-Topics (env KMG_TOPICS)")
    p.add_argument("--tag", action="append", default=[t for t in env("KMG_TAGS", "").split(",") if "=" in t],
                   help="zusätzlicher Tag k=v für alle Metriken, wie Telegraf global_tags (mehrfach; env KMG_TAGS=k=v,k2=v2)")
    p.add_argument("--heap-gb", type=float, default=float(env("KMG_HEAP_GB", 6)), help="JVM-Heap der Broker in GiB")
    p.add_argument("--gc-threads", type=int, default=8, help="GC-Threads (java_last_garbage_collection)")
    p.add_argument("--zookeeper", action="store_true", default=env_bool("KMG_ZOOKEEPER"),
                   help="ZooKeeper-Modus: zusätzlich kafka_zookeeper (env KMG_ZOOKEEPER=1)")
    p.add_argument("--string-fields", action="store_true", default=env_bool("KMG_STRING_FIELDS"),
                   help="Text-Felder wie Telegraf mitsenden (EventType, RateUnit, status) - "
                        "VictoriaMetrics legt dafür nutzlose 0-Zeitreihen an (env KMG_STRING_FIELDS=1)")
    p.add_argument("--no-burrow", action="store_true", default=env_bool("KMG_NO_BURROW"), help="keine Burrow-Metriken")
    p.add_argument("--no-java", action="store_true", default=env_bool("KMG_NO_JAVA"), help="keine JVM-Metriken")
    p.add_argument("--no-kafka", action="store_true", default=env_bool("KMG_NO_KAFKA"), help="keine kafka_*-Metriken")
    p.add_argument("--state-file", default=env("KMG_STATE_FILE", ""),
                   help="Datei für Topic- und Consumer-Offsets, damit sie Neustarts überleben (env KMG_STATE_FILE)")
    p.add_argument("--backfill", type=int, default=int(env("KMG_BACKFILL", 0)),
                   help="vor dem Live-Betrieb N Minuten Historie erzeugen (env KMG_BACKFILL)")
    p.add_argument("--duration", type=int, default=0, help="nach N Sekunden beenden (0 = endlos)")
    p.add_argument("--timeout", type=float, default=10.0, help="HTTP-Timeout in s")
    p.add_argument("--max-buffer", type=int, default=int(env("KMG_MAX_BUFFER", 500_000)),
                   help="max. gepufferte Zeilen bei Verbindungsfehlern")
    p.add_argument("--stdout", action="store_true", help="Line Protocol zusätzlich auf stdout ausgeben")
    p.add_argument("--no-send", action="store_true", help="nichts senden (Testmodus)")
    p.add_argument("--seed", type=int, default=None, help="Zufalls-Seed")
    p.add_argument("-v", "--verbose", action="store_true", help="Debug-Ausgaben")
    a = p.parse_args()
    if a.brokers < 1 or a.interval < 1:
        p.error("--brokers und --interval müssen >= 1 sein")
    return a


def main():
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)
    iv = args.interval
    now = time.time()
    start = (math.floor(now / iv) * iv) - args.backfill * 60
    cluster = Cluster(args, start)
    cluster.next_incident = start + cluster.rnd.uniform(240, 900)
    if args.state_file:
        cluster.load_state(args.state_file)
    sender = None if args.no_send else VMSender(args.url, args.path, args.user, args.password,
                                                args.bearer_token, args.timeout, args.max_buffer)
    running = True

    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    def emit(lines):
        if args.stdout:
            try:
                sys.stdout.write("\n".join(lines) + "\n")
                sys.stdout.flush()
            except BrokenPipeError:
                stop()
                return
        if sender:
            sender.send(lines)

    def collect(ts):
        cluster.step(ts, iv)
        ns = int(ts) * 1_000_000_000
        return [ln for m, t, f in cluster.points(ts) if (ln := to_line(m, t, f, ns))]

    log.info("Starte: %d Broker, %d Topics/%d Partitionen, %d Consumer-Gruppen, Intervall %d s, Ziel %s",
             args.brokers, len(cluster.topic_parts), len(cluster.parts), len(cluster.groups), iv,
             "(kein Versand)" if args.no_send else sender.url)

    ts = start
    if args.backfill > 0:
        log.info("Erzeuge Historie für %d Minuten ...", args.backfill)
        batch = []
        while ts + iv <= now and running:
            ts += iv
            batch += collect(ts)
            if len(batch) >= 40_000:
                emit(batch)
                batch = []
        if batch:
            emit(batch)
        log.info("Historie fertig (%d Zeilen gesendet).", sender.sent if sender else 0)
    else:
        ts = math.floor(now / iv) * iv

    t0 = time.time()
    stat_lines, stat_t = 0, time.time()
    last_save = time.time()
    if args.state_file and args.backfill > 0:
        cluster.save_state(args.state_file, ts)
    while running:
        ts += iv
        delay = ts - time.time()
        if delay > 0:
            time.sleep(delay)
        elif delay < -5 * iv:                       # nach Suspend nicht nachholen
            ts = math.floor(time.time() / iv) * iv
        lines = collect(ts)
        emit(lines)
        stat_lines += len(lines)
        if args.state_file and time.time() - last_save >= 60:
            cluster.save_state(args.state_file, ts)
            last_save = time.time()
        if time.time() - stat_t >= 60:
            msg = "%d Zeilen in den letzten 60 s" % stat_lines
            if sender:
                msg += ", gesendet gesamt: %d, im Puffer: %d" % (sender.sent, len(sender.buffer))
                if sender.dropped:
                    msg += ", verworfen: %d" % sender.dropped
            log.info(msg)
            stat_lines, stat_t = 0, time.time()
        if args.duration and time.time() - t0 >= args.duration:
            break
    if sender and sender.buffer:
        sender.send([])
    if args.state_file:
        cluster.save_state(args.state_file, ts)
    log.info("Beendet.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
