#!/usr/bin/env python3
"""
pg_metrics_gen.py - Generator für PostgreSQL-Metriken, wie Telegraf (inputs.postgresql)
                    sie erfasst, mit Versand an VictoriaMetrics (Influx Line Protocol).

Nachgebildet wird genau, was das Telegraf-Plugin "postgresql" liefert:
  * SELECT * FROM pg_stat_database  -> eine Zeile je Datenbank
  * SELECT * FROM pg_stat_bgwriter  -> eine Zeile je Server (Tag db = Verbindungs-DB "postgres")
  Messgröße "postgresql", Tags: host, server (bereinigte Verbindungsadresse), db.
  Alle Spalten außer stats_reset werden Felder (Zeitstempel-Spalten verwirft Telegraf).
  In VictoriaMetrics heißen die Metriken z.B. postgresql_xact_commit, postgresql_blks_hit.

Spalten je nach PostgreSQL-Version (--pg-version, Standard 16):
  12+  checksum_failures             14+  sessions, session_time, active_time, ...
  17+  pg_stat_bgwriter ohne Checkpoint-Spalten (Telegraf liest pg_stat_checkpointer nicht)
  18+  parallel_workers_to_launch, parallel_workers_launched

Simuliert werden 4 Hosts mit je 8 Anwendungsdatenbanken (plus postgres, template0,
template1 und der Zeile postgres_global wie bei Telegrafs Standardkonfiguration),
mit Tagesgang und Störfällen: Lock-Konflikte/Deadlocks, Cache-Misses, Verbindungs-
sturm, Batch-Ladevorgang, Absturz eines Servers (Statistik-Reset).

Die Zähler überleben Neustarts des Generators über eine Statusdatei (--state-file),
so wie PostgreSQL seine Statistiken bei einem sauberen Neustart behält.

Nur Python-Standardbibliothek (Python >= 3.9).

Beispiele:
  ./pg_metrics_gen.py --url http://victoriametrics:8428
  ./pg_metrics_gen.py --backfill 120 --state-file /tmp/pg-state.json
  ./pg_metrics_gen.py --stdout --no-send
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
import urllib.request

log = logging.getLogger("pg-metrics-gen")

# ---------------------------------------------------------------------------
# Hosts und Datenbanken
# ---------------------------------------------------------------------------
# Profile: tps (Commits/s zur Spitzenzeit), Lese-/Schreibverhalten je Transaktion,
# Cache-Trefferquote, Verbindungspool, Tagesgang ("day" = Bürozeiten, "evening" = Shop,
# "night" = nächtliche Batch-Jobs, "flat" = gleichmäßig)
PROFILES = {
    #            tps   fetch  ret    ins   upd   del   hit     conns    rollb  temp/h  peak
    "oltp":    (220,   12,    40,    1.2,  0.8,  0.1,  0.998,  (20, 60), 0.012, 0.5,   "evening"),
    "web":     (90,    8,     30,    0.3,  0.4,  0.05, 0.997,  (10, 30), 0.008, 0.2,   "day"),
    "queue":   (150,   3,     10,    1.0,  1.5,  0.9,  0.999,  (8, 20),  0.004, 0.0,   "flat"),
    "small":   (8,     6,     25,    0.1,  0.2,  0.02, 0.995,  (3, 10),  0.006, 0.05,  "day"),
    "analytic": (12,   400,   9000,  2.0,  0.2,  0.1,  0.93,   (4, 16),  0.02,  12.0,  "night"),
}
DEFAULT_HOSTS = {
    "pg-1": [("shop", "oltp"), ("orders", "oltp"), ("payments", "oltp"), ("inventory", "oltp"),
             ("customers", "web"), ("catalog", "web"), ("sessions", "queue"), ("jobs", "queue")],
    "pg-2": [("keycloak", "web"), ("grafana", "small"), ("gitea", "small"), ("nextcloud", "web"),
             ("wiki", "small"), ("cms", "small"), ("mattermost", "web"), ("zabbix", "queue")],
    "pg-3": [("analytics", "analytic"), ("reporting", "analytic"), ("dwh_staging", "analytic"),
             ("events", "queue"), ("metrics", "queue"), ("audit", "web"), ("archive", "small"),
             ("ml_features", "analytic")],
    "pg-4": [("billing", "oltp"), ("crm", "web"), ("erp", "oltp"), ("hr", "small"),
             ("documents", "web"), ("notifications", "queue"), ("scheduler", "queue"),
             ("search_index", "analytic")],
}
DB_COUNTERS = ["xact_commit", "xact_rollback", "blks_read", "blks_hit", "tup_returned", "tup_fetched",
               "tup_inserted", "tup_updated", "tup_deleted", "conflicts", "temp_files", "temp_bytes",
               "deadlocks", "checksum_failures", "blk_read_time", "blk_write_time", "session_time",
               "active_time", "idle_in_transaction_time", "sessions", "sessions_abandoned", "sessions_fatal",
               "sessions_killed", "parallel_workers_to_launch", "parallel_workers_launched"]
FLOAT_COLS = {"blk_read_time", "blk_write_time", "session_time", "active_time", "idle_in_transaction_time",
              "checkpoint_write_time", "checkpoint_sync_time"}
BG_COUNTERS = ["checkpoints_timed", "checkpoints_req", "checkpoint_write_time", "checkpoint_sync_time",
               "buffers_checkpoint", "buffers_clean", "maxwritten_clean", "buffers_backend",
               "buffers_backend_fsync", "buffers_alloc"]


def poisson(rnd: random.Random, lam: float) -> int:
    if lam <= 0:
        return 0
    if lam > 30:
        return max(0, int(round(rnd.gauss(lam, math.sqrt(lam)))))
    L, k, p = math.exp(-lam), 0, 1.0
    while True:
        p *= rnd.random()
        if p <= L:
            return k
        k += 1


def load_factor(ts: float, peak: str) -> float:
    lt = time.localtime(ts)
    h = lt.tm_hour + lt.tm_min / 60
    weekend = lt.tm_wday >= 5
    if peak == "flat":
        f = 0.8 + 0.2 * math.cos((h - 13) / 24 * 2 * math.pi)
    elif peak == "night":                       # Batch-Fenster 1-5 Uhr
        f = 0.25 + (0.9 if 1 <= h < 5 else 0.0) + 0.15 * math.cos((h - 11) / 24 * 2 * math.pi)
    elif peak == "evening":                     # Shop: Spitze am Abend
        f = 0.5 + 0.5 * math.cos((h - 20) / 24 * 2 * math.pi)
        if weekend:
            f *= 1.15
    else:                                       # Bürozeiten
        f = 0.12 + (0.88 if 8 <= h < 18 else 0.0) * (0.75 + 0.25 * math.sin((h - 8) / 10 * math.pi))
        if weekend:
            f *= 0.25
    return max(0.05, f)


class Database:
    def __init__(self, rnd, name, profile, oid, age_s):
        self.rnd, self.name, self.oid = rnd, name, oid
        self.kind = profile
        (self.tps, self.fetch, self.ret, self.ins, self.upd, self.dele, self.hit, self.pool,
         self.rollback, self.temp_h, self.peak) = PROFILES[profile]
        self.tps *= rnd.uniform(0.6, 1.4)
        self.hit = min(0.9995, self.hit * rnd.uniform(0.998, 1.001))
        self.c = {k: 0.0 for k in DB_COUNTERS}
        # Startwerte: Statistiken laufen seit 'age_s' Sekunden (seit stats_reset)
        avg = 0.45
        commits = self.tps * avg * age_s
        self._advance(commits, avg_only=True)
        self.numbackends = self.pool[0]
        # Störfall-Faktoren
        self.f_rollback = self.f_conns = self.f_ins = self.f_temp = 1.0
        self.deadlock_rate = 0.0  # zusätzliche Deadlocks/s im Lock-Störfall
        self.f_hit = 0.0          # Abzug von der Trefferquote
        self.f_io = 1.0

    def _advance(self, commits, dt=None, load=None, avg_only=False):
        c = self.c
        c["xact_commit"] += commits
        c["xact_rollback"] += commits * self.rollback
        fetched, returned = commits * self.fetch, commits * self.ret
        ins, upd, dele = commits * self.ins, commits * self.upd, commits * self.dele
        c["tup_fetched"] += fetched
        c["tup_returned"] += returned
        c["tup_inserted"] += ins
        c["tup_updated"] += upd
        c["tup_deleted"] += dele
        blocks = fetched / 40 + returned / 120 + (ins + upd + dele) / 20 + commits * 2
        c["blks_hit"] += blocks * self.hit
        c["blks_read"] += blocks * (1 - self.hit)
        c["blk_read_time"] += blocks * (1 - self.hit) * 0.12
        c["blk_write_time"] += (ins + upd + dele) / 20 * 0.03
        secs = commits / max(self.tps * 0.45, 1e-9)
        conns = sum(self.pool) / 2
        c["session_time"] += conns * secs * 1000
        c["active_time"] += conns * secs * 1000 * 0.12
        c["idle_in_transaction_time"] += conns * secs * 1000 * 0.004
        c["sessions"] += secs * conns / 900                       # Verbindungen leben ~15 min
        c["temp_files"] += secs / 3600 * self.temp_h
        c["temp_bytes"] += secs / 3600 * self.temp_h * 48 * 1024 * 1024
        c["deadlocks"] += secs / 86400 * (2 if self.kind == "oltp" else 0.2)
        c["sessions_abandoned"] += secs / 86400 * 3
        c["sessions_killed"] += secs / 86400 * 0.2
        if self.kind == "analytic":
            c["parallel_workers_to_launch"] += commits * 0.4
            c["parallel_workers_launched"] += commits * 0.38
        if avg_only:
            for k in c:
                c[k] = float(int(c[k])) if k not in FLOAT_COLS else c[k]
        return blocks

    def step(self, ts, dt, host_crashed=False):
        r, c = self.rnd, self.c
        load = load_factor(ts, self.peak)
        lam = self.tps * load * dt
        commits = max(0, int(round(r.gauss(lam, math.sqrt(lam) + 0.5))))
        rollbacks = poisson(r, commits * self.rollback * self.f_rollback)
        c["xact_commit"] += commits
        c["xact_rollback"] += rollbacks
        n = commits + rollbacks
        fetched = poisson(r, n * self.fetch * r.uniform(0.8, 1.2))
        returned = poisson(r, n * self.ret * r.uniform(0.7, 1.3))
        ins = poisson(r, n * self.ins * self.f_ins)
        upd = poisson(r, n * self.upd)
        dele = poisson(r, n * self.dele)
        c["tup_fetched"] += fetched
        c["tup_returned"] += returned
        c["tup_inserted"] += ins
        c["tup_updated"] += upd
        c["tup_deleted"] += dele
        blocks = fetched / 40 + returned / 120 + (ins + upd + dele) / 20 + n * 2
        hit = max(0.5, self.hit - self.f_hit)
        read = poisson(r, blocks * (1 - hit))
        c["blks_hit"] += int(blocks - read) if blocks > read else 0
        c["blks_read"] += read
        c["blk_read_time"] += read * r.uniform(0.06, 0.2) * self.f_io
        wblocks = (ins + upd + dele) / 20
        c["blk_write_time"] += wblocks * r.uniform(0.01, 0.05) * self.f_io
        # Verbindungen
        lo, hi = self.pool
        target = (lo + (hi - lo) * min(load, 1.2)) * self.f_conns
        self.numbackends += (target - self.numbackends) * 0.3 + r.gauss(0, 0.6)
        self.numbackends = max(1.0 if self.kind != "small" else 0.0, self.numbackends)
        nb = self.numbackends
        active_frac = min(0.95, 0.04 + 0.25 * load * (1 + (self.f_rollback - 1) * 0.3))
        c["session_time"] += nb * dt * 1000
        c["active_time"] += nb * dt * 1000 * active_frac
        c["idle_in_transaction_time"] += nb * dt * 1000 * (0.004 * self.f_rollback)
        c["sessions"] += poisson(r, nb * dt / 900 * (1 + self.f_conns - 1))
        c["sessions_abandoned"] += poisson(r, dt / 86400 * 3)
        c["sessions_killed"] += poisson(r, dt / 86400 * 0.2)
        # Temp-Dateien (große Sortierungen/Hashes)
        tf = poisson(r, dt / 3600 * self.temp_h * load * self.f_temp)
        if tf:
            c["temp_files"] += tf
            c["temp_bytes"] += sum(int(r.lognormvariate(math.log(24 * 1024 * 1024), 1.0)) for _ in range(tf))
        c["deadlocks"] += poisson(r, dt / 86400 * (2 if self.kind == "oltp" else 0.2) + dt * self.deadlock_rate)
        if self.kind == "analytic":
            pw = poisson(r, n * 0.4)
            c["parallel_workers_to_launch"] += pw
            c["parallel_workers_launched"] += pw - poisson(r, pw * 0.03)
        return read, wblocks

    def fields(self, version, strings):
        c = self.c
        f = {"datid": self.oid, "numbackends": int(round(self.numbackends))}
        cols = [k for k in DB_COUNTERS if
                not (k == "checksum_failures" and version < 12) and
                not (k in ("session_time", "active_time", "idle_in_transaction_time", "sessions",
                           "sessions_abandoned", "sessions_fatal", "sessions_killed") and version < 14) and
                not (k.startswith("parallel_workers") and version < 18)]
        for k in cols:
            f[k] = float(c[k]) if k in FLOAT_COLS else int(c[k])
        if strings:
            f["datname"] = self.name
        return f


class SystemDatabase(Database):
    """postgres, template0, template1: kaum Aktivität (postgres: Telegraf selbst)."""

    def __init__(self, rnd, name, oid, age_s):
        self.rnd, self.name, self.oid, self.kind = rnd, name, oid, "system"
        self.c = {k: 0.0 for k in DB_COUNTERS}
        self.numbackends = 1.0 if name == "postgres" else 0.0
        if name == "postgres":
            self.c.update(xact_commit=float(int(age_s * 0.25)), tup_returned=float(int(age_s * 60)),
                          tup_fetched=float(int(age_s * 9)), blks_hit=float(int(age_s * 40)),
                          blks_read=float(int(age_s * 0.02)), sessions=float(int(age_s / 3600)),
                          session_time=age_s * 1000.0, active_time=age_s * 6.0)

    def step(self, ts, dt, host_crashed=False):
        if self.name != "postgres":
            return 0, 0
        r, c = self.rnd, self.c
        q = 2 * dt / 10                              # Telegraf: 2 Abfragen je Intervall
        c["xact_commit"] += round(q)
        c["tup_returned"] += poisson(self.rnd, 600 * q)
        c["tup_fetched"] += poisson(self.rnd, 90 * q)
        c["blks_hit"] += poisson(self.rnd, 400 * q)
        c["blks_read"] += poisson(self.rnd, 0.05 * q)
        c["session_time"] += dt * 1000
        c["active_time"] += dt * 6
        self.numbackends = 1 + (r.random() < 0.15)
        return 0, 0


class GlobalRow:
    """Zeile mit datid=0 (gemeinsame Kataloge, ab PG 12) -> Telegraf: db=postgres_global."""

    def __init__(self, age_s):
        self.name = "postgres_global"
        self.c = {"blks_read": float(int(age_s * 0.01)), "blks_hit": float(int(age_s * 30)),
                  "tup_returned": float(int(age_s * 5)), "tup_fetched": float(int(age_s * 12)),
                  "tup_inserted": 0.0, "tup_updated": float(int(age_s * 0.001)), "tup_deleted": 0.0}

    def step(self, rnd, dt):
        self.c["blks_hit"] += poisson(rnd, 30 * dt)
        self.c["blks_read"] += poisson(rnd, 0.01 * dt)
        self.c["tup_returned"] += poisson(rnd, 5 * dt)
        self.c["tup_fetched"] += poisson(rnd, 12 * dt)

    def fields(self, version, strings):
        # NULL-Spalten (numbackends, xact_* ...) liefert Telegraf nicht -> nur befüllte Spalten
        f = {"datid": 0}
        f.update({k: int(v) for k, v in self.c.items()})
        f.update({"conflicts": 0, "temp_files": 0, "temp_bytes": 0, "deadlocks": 0,
                  "blk_read_time": 0.0, "blk_write_time": 0.0})
        if version >= 12:
            f["checksum_failures"] = 0
        return f


class Host:
    def __init__(self, rnd, name, dbs, args):
        self.rnd, self.name, self.args = rnd, name, args
        age = rnd.uniform(10, 200) * 86400
        self.dbs: list = [Database(rnd, n, p, 16384 + i * 7 + rnd.randint(0, 6), age * rnd.uniform(0.6, 1.0))
                          for i, (n, p) in enumerate(dbs)]
        self.dbs += [SystemDatabase(rnd, "postgres", 5, age), SystemDatabase(rnd, "template1", 1, age),
                     SystemDatabase(rnd, "template0", 4, age)]
        self.global_row = GlobalRow(age)
        self.bg = {k: 0.0 for k in BG_COUNTERS}
        self.bg.update(checkpoints_timed=float(int(age / 300)), checkpoints_req=float(int(age / 86400 * 3)),
                       checkpoint_write_time=age / 300 * 270000.0 * 0.6, checkpoint_sync_time=age / 300 * 40.0,
                       buffers_checkpoint=float(int(age * 30)), buffers_clean=float(int(age * 8)),
                       maxwritten_clean=float(int(age / 3000)), buffers_backend=float(int(age * 5)),
                       buffers_alloc=float(int(age * 25)))
        self.next_checkpoint = 0.0
        self.dirty = 0.0
        self.online = True
        self.down_until = 0.0
        pools = sum(d.pool[1] for d in self.dbs if not isinstance(d, SystemDatabase))
        self.max_connections = int(math.ceil(pools * 1.3 / 50.0) * 50)       # Reserve wie im Betrieb üblich
        self.write_avg = None

    def reset_stats(self):
        """Absturz: PostgreSQL verwirft die Statistiken (Crash-Recovery)."""
        for db in self.dbs:
            db.c = {k: 0.0 for k in db.c}
            db.numbackends = 0.0
        self.global_row.c = {k: 0.0 for k in self.global_row.c}
        self.bg = {k: 0.0 for k in BG_COUNTERS}

    def step(self, ts, dt):
        r = self.rnd
        if self.next_checkpoint == 0.0:
            self.next_checkpoint = ts + r.uniform(0, 300)
        reads = writes = 0.0
        for db in self.dbs:
            rd, wr = db.step(ts, dt)
            reads += rd
            writes += wr
        self.global_row.step(r, dt)
        # Verbindungslimit
        total = sum(db.numbackends for db in self.dbs)
        if total > self.max_connections:
            over = total - self.max_connections
            busiest = max(self.dbs, key=lambda d: d.numbackends)
            busiest.c["sessions_fatal"] += poisson(r, over * dt / 5)
            for db in self.dbs:
                db.numbackends *= self.max_connections / total
        # bgwriter / Checkpoints
        bg = self.bg
        self.dirty += writes * r.uniform(0.6, 0.9)
        bg["buffers_alloc"] += int(reads) + poisson(r, 3 * dt)
        clean = poisson(r, writes * 0.15)
        bg["buffers_clean"] += clean
        bg["maxwritten_clean"] += poisson(r, dt / 3600 * (2 if writes / dt > 300 else 0.1))
        bg["buffers_backend"] += poisson(r, writes * 0.05)
        rate_w = writes / dt
        self.write_avg = rate_w if self.write_avg is None else self.write_avg + (rate_w - self.write_avg) * dt / 7200
        wal_heavy = rate_w > 1.6 * max(self.write_avg, 1.0)   # deutlich mehr WAL als üblich -> max_wal_size
        if wal_heavy and r.random() < dt / 120:
            self._checkpoint(requested=True)
        if ts >= self.next_checkpoint:
            self._checkpoint(requested=False)
            self.next_checkpoint = ts + 300               # checkpoint_timeout = 5min

    def _checkpoint(self, requested):
        r, bg = self.rnd, self.bg
        bg["checkpoints_req" if requested else "checkpoints_timed"] += 1
        written = int(self.dirty) + poisson(self.rnd, 200)
        bg["buffers_checkpoint"] += written
        bg["checkpoint_write_time"] += (r.uniform(20000, 90000) if requested else
                                        min(270000.0, written * 0.4 + r.uniform(200, 3000)))
        bg["checkpoint_sync_time"] += r.uniform(5, 60) + written * 0.002
        self.dirty = 0.0

    def bg_fields(self, version):
        cols = ["buffers_clean", "maxwritten_clean", "buffers_alloc"] if version >= 17 else BG_COUNTERS
        return {k: (float(self.bg[k]) if k in FLOAT_COLS else int(self.bg[k])) for k in cols}


class Simulation:
    def __init__(self, args, now):
        self.args = args
        self.rnd = rnd = random.Random(args.seed)
        hosts = DEFAULT_HOSTS
        if args.hosts != 4 or args.dbs_per_host != 8:
            names = [n for lst in DEFAULT_HOSTS.values() for n in lst]
            hosts = {}
            for h in range(args.hosts):
                hosts[f"{args.host_prefix}{h + 1}"] = [
                    (names[(h * args.dbs_per_host + i) % len(names)][0] +
                     ("" if h * args.dbs_per_host + i < len(names) else f"_{h + 1}"),
                     names[(h * args.dbs_per_host + i) % len(names)][1]) for i in range(args.dbs_per_host)]
        elif args.host_prefix != "pg-":
            hosts = {f"{args.host_prefix}{i + 1}": v for i, v in enumerate(DEFAULT_HOSTS.values())}
        self.hosts = [Host(rnd, h, dbs, args) for h, dbs in hosts.items()]
        self.next_incident = now + rnd.uniform(240, 900)
        self.incident = None
        self.ignored = {d.strip() for d in args.ignored_databases.split(",") if d.strip()}

    # -- Statusdatei ------------------------------------------------------------
    def save_state(self, path, now):
        state = {"version": 1, "saved": now, "hosts": {}}
        for h in self.hosts:
            state["hosts"][h.name] = {"dbs": {db.name: db.c for db in h.dbs},
                                      "global": h.global_row.c, "bg": h.bg}
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
            return
        except (OSError, ValueError) as e:
            log.warning("Statusdatei %s nicht lesbar (%s) - beginne neu.", path, e)
            return
        n = 0
        for h in self.hosts:
            sh = state.get("hosts", {}).get(h.name)
            if not sh:
                continue
            for db in h.dbs:
                if db.name in sh.get("dbs", {}):
                    db.c.update({k: float(v) for k, v in sh["dbs"][db.name].items() if k in db.c})
                    n += 1
            h.global_row.c.update({k: float(v) for k, v in sh.get("global", {}).items() if k in h.global_row.c})
            h.bg.update({k: float(v) for k, v in sh.get("bg", {}).items() if k in h.bg})
        log.info("Zähler aus %s übernommen (%d Datenbanken, Stand %s).", path, n,
                 time.strftime("%d.%m. %H:%M", time.localtime(state.get("saved", 0))))

    # -- Störfälle ----------------------------------------------------------------
    def update_incidents(self, now):
        r = self.rnd
        stamp = time.strftime("%d.%m. %H:%M:%S", time.localtime(now))
        if self.incident and now >= self.incident[1]:
            kind, _e, host, db = self.incident
            if kind == "crash":
                host.online = True
            elif db is not None:
                db.f_rollback = db.f_conns = db.f_ins = db.f_temp = db.f_io = 1.0
                db.deadlock_rate = 0.0
                db.f_hit = 0.0
            log.info("[%s] Störfall beendet: %s (%s/%s)", stamp, kind, host.name, db.name if db else "-")
            self.incident = None
            self.next_incident = now + r.uniform(600, 2400)
        if self.incident is None and now >= self.next_incident:
            kind = r.choices(["locks", "cache", "conn_storm", "batch", "crash"], [0.3, 0.2, 0.2, 0.22, 0.08])[0]
            host = r.choice(self.hosts)
            apps = [d for d in host.dbs if isinstance(d, Database) and not isinstance(d, SystemDatabase)]
            db = None
            if kind == "locks":
                db = r.choice([d for d in apps if d.kind in ("oltp", "web")] or apps)
                db.f_rollback, db.f_conns = r.uniform(4, 10), r.uniform(1.5, 2.5)
                db.deadlock_rate = r.uniform(0.01, 0.05)          # ca. 3-15 Deadlocks in 5 min
                dur = r.uniform(180, 600)
            elif kind == "cache":
                db = r.choice(apps)
                db.f_hit, db.f_io, db.f_temp = r.uniform(0.05, 0.15), r.uniform(2, 5), 4.0
                dur = r.uniform(300, 900)
            elif kind == "conn_storm":
                db = r.choice(apps)
                others = sum(d.numbackends for d in host.dbs if d is not db)
                db.f_conns = max(2.0, (host.max_connections - others) / max(1.0, db.numbackends)) * r.uniform(1.0, 1.3)
                dur = r.uniform(120, 420)
            elif kind == "batch":
                db = r.choice([d for d in apps if d.kind in ("analytic", "queue", "oltp")] or apps)
                db.f_ins, db.f_temp, db.f_io = r.uniform(15, 40), 6.0, 1.5
                dur = r.uniform(300, 1200)
            else:
                host.online = False
                host.reset_stats()
                dur = r.uniform(30, 120)
            self.incident = (kind, now + dur, host, db)
            log.info("[%s] Störfall: %s (%s/%s) für %.0f s", stamp, kind, host.name, db.name if db else "-", dur)

    def step(self, now, dt):
        self.update_incidents(now)
        for h in self.hosts:
            if h.online:
                h.step(now, dt)

    def points(self):
        a = self.args
        out = []
        extra = dict(t.split("=", 1) for t in a.tag) if a.tag else {}
        for h in self.hosts:
            if not h.online:
                continue                                    # Server nicht erreichbar -> keine Daten
            base = {**extra, "host": h.name, "server": a.server_tag}
            for db in h.dbs:
                if db.name in self.ignored:
                    continue
                out.append(("postgresql", {**base, "db": db.name}, db.fields(a.pg_version, a.string_fields)))
            if a.pg_version >= 12 and not self.ignored:      # NULL-datname fällt bei "NOT IN" heraus
                out.append(("postgresql", {**base, "db": "postgres_global"},
                            h.global_row.fields(a.pg_version, a.string_fields)))
            out.append(("postgresql", {**base, "db": a.connection_db}, h.bg_fields(a.pg_version)))
        return out


# ---------------------------------------------------------------------------
# Influx Line Protocol + Versand (wie beim Kafka-Generator)
# ---------------------------------------------------------------------------

def _esc(s: str, chars=",= ") -> str:
    s = s.replace("\\", "\\\\")
    for c in chars:
        s = s.replace(c, "\\" + c)
    return s


def to_line(measurement, tags, fields, ts_ns):
    fs = []
    for k, v in fields.items():
        if v is None:
            continue
        if isinstance(v, bool):
            val = "true" if v else "false"
        elif isinstance(v, int):
            val = f"{v}i"                                   # bigint/int/oid -> Ganzzahl wie bei Telegraf
        elif isinstance(v, float):
            if math.isnan(v) or math.isinf(v):
                continue
            val = repr(v)                                   # double precision
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
    p = argparse.ArgumentParser(description="PostgreSQL-Metrik-Generator (Telegraf-Format) für VictoriaMetrics",
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--url", default=env("VM_URL", "http://localhost:8428"), help="VictoriaMetrics-URL (env VM_URL)")
    p.add_argument("--path", default=env("VM_WRITE_PATH", "/write"),
                   help="Schreibpfad; Cluster: /insert/<tenant>/influx/write (env VM_WRITE_PATH)")
    p.add_argument("--user", default=env("VM_USER", None), help="Basic-Auth-Benutzer (env VM_USER)")
    p.add_argument("--password", default=env("VM_PASSWORD", None), help="Basic-Auth-Passwort (env VM_PASSWORD)")
    p.add_argument("--bearer-token", default=env("VM_BEARER_TOKEN", None), help="Bearer-Token (env VM_BEARER_TOKEN)")
    p.add_argument("--interval", type=int, default=int(env("PMG_INTERVAL", 10)),
                   help="Erfassungsintervall in s wie Telegraf 'interval' (env PMG_INTERVAL)")
    p.add_argument("--hosts", type=int, default=int(env("PMG_HOSTS", 4)), help="Anzahl Hosts (env PMG_HOSTS)")
    p.add_argument("--dbs-per-host", type=int, default=int(env("PMG_DBS_PER_HOST", 8)),
                   help="Anwendungsdatenbanken je Host (env PMG_DBS_PER_HOST)")
    p.add_argument("--host-prefix", default=env("PMG_HOST_PREFIX", "pg-"), help="Hostnamen <prefix>1..N")
    p.add_argument("--pg-version", type=int, default=int(env("PMG_PG_VERSION", 16)),
                   help="PostgreSQL-Hauptversion, bestimmt die Spalten (env PMG_PG_VERSION)")
    p.add_argument("--server-tag", default=env("PMG_SERVER_TAG", "host=localhost user=telegraf dbname=postgres"),
                   help="Tag 'server' = bereinigte Telegraf-Verbindungsadresse (env PMG_SERVER_TAG)")
    p.add_argument("--connection-db", default=env("PMG_CONNECTION_DB", "postgres"),
                   help="Verbindungs-DB, unter der pg_stat_bgwriter erscheint")
    p.add_argument("--ignored-databases", default=env("PMG_IGNORED_DATABASES", ""),
                   help="wie Telegraf ignored_databases, z.B. template0,template1 (env PMG_IGNORED_DATABASES)")
    p.add_argument("--tag", action="append", default=[t for t in env("PMG_TAGS", "").split(",") if "=" in t],
                   help="zusätzlicher Tag k=v wie Telegraf global_tags (mehrfach; env PMG_TAGS=k=v,k2=v2)")
    p.add_argument("--string-fields", action="store_true", default=env_bool("PMG_STRING_FIELDS"),
                   help="Text-Feld datname wie Telegraf mitsenden (erzeugt 0-Zeitreihen) (env PMG_STRING_FIELDS=1)")
    p.add_argument("--state-file", default=env("PMG_STATE_FILE", ""),
                   help="Datei, in der die Zähler Neustarts überleben (env PMG_STATE_FILE)")
    p.add_argument("--backfill", type=int, default=int(env("PMG_BACKFILL", 0)),
                   help="vor dem Live-Betrieb N Minuten Historie erzeugen (env PMG_BACKFILL)")
    p.add_argument("--duration", type=int, default=0, help="nach N Sekunden beenden (0 = endlos)")
    p.add_argument("--timeout", type=float, default=10.0, help="HTTP-Timeout in s")
    p.add_argument("--max-buffer", type=int, default=int(env("PMG_MAX_BUFFER", 200_000)),
                   help="max. gepufferte Zeilen bei Verbindungsfehlern")
    p.add_argument("--stdout", action="store_true", help="Line Protocol zusätzlich auf stdout ausgeben")
    p.add_argument("--no-send", action="store_true", help="nichts senden (Testmodus)")
    p.add_argument("--seed", type=int, default=None, help="Zufalls-Seed")
    p.add_argument("-v", "--verbose", action="store_true", help="Debug-Ausgaben")
    a = p.parse_args()
    if a.hosts < 1 or a.dbs_per_host < 1 or a.interval < 1:
        p.error("--hosts, --dbs-per-host und --interval müssen >= 1 sein")
    return a


def main():
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)
    iv = args.interval
    now = time.time()
    start = math.floor(now / iv) * iv - args.backfill * 60
    sim = Simulation(args, start)
    if args.state_file:
        sim.load_state(args.state_file)
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
        sim.step(ts, iv)
        ns = int(ts) * 1_000_000_000
        return [ln for m, t, f in sim.points() if (ln := to_line(m, t, f, ns))]

    ndb = sum(len(h.dbs) for h in sim.hosts)
    log.info("Starte: %d Hosts, %d Datenbanken (inkl. postgres/template0/template1), PostgreSQL %d, "
             "Intervall %d s, Ziel %s", len(sim.hosts), ndb, args.pg_version, iv,
             "(kein Versand)" if args.no_send else sender.url)

    ts = start
    if args.backfill > 0:
        log.info("Erzeuge Historie für %d Minuten ...", args.backfill)
        batch = []
        while ts + iv <= now and running:
            ts += iv
            batch += collect(ts)
            if len(batch) >= 20_000:
                emit(batch)
                batch = []
        if batch:
            emit(batch)
        log.info("Historie fertig (%d Zeilen gesendet).", sender.sent if sender else 0)
    else:
        ts = math.floor(now / iv) * iv

    t0 = last_save = stat_t = time.time()
    stat_lines = 0
    if args.state_file and args.backfill > 0:
        sim.save_state(args.state_file, ts)
    while running:
        ts += iv
        delay = ts - time.time()
        if delay > 0:
            time.sleep(delay)
        elif delay < -5 * iv:
            ts = math.floor(time.time() / iv) * iv
        lines = collect(ts)
        emit(lines)
        stat_lines += len(lines)
        if args.state_file and time.time() - last_save >= 60:
            sim.save_state(args.state_file, ts)
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
        sim.save_state(args.state_file, ts)
    log.info("Beendet.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
