#!/usr/bin/env python3
"""
apache_loggen_opensearch.py - Generator für realistische Apache-Access-Logs im
                              JSON-Format (LogFormat "logstash_json") mit Versand
                              an OpenSearch.

Erzeugt pro Sekunde zufällig MIN..MAX Logeinträge (Standard 2..20) und schreibt
sie gebündelt über die Bulk-API in Tagesindizes (Standard: apache-logs-JJJJ.MM.TT).
Beim Start wird ein Index-Template mit passenden Feldtypen angelegt.

Nur Python-Standardbibliothek, keine zusätzlichen Pakete nötig (Python >= 3.9).

Beispiele:
  ./apache_loggen_opensearch.py --url https://opensearch:9200 --user admin --password '...'
  ./apache_loggen_opensearch.py --url https://localhost:9200 --insecure   # Demo-Zertifikate
  ./apache_loggen_opensearch.py --stdout --no-send                       # nur Konsole
  ./apache_loggen_opensearch.py --backfill 60                            # 60 Min. Historie

Alle Optionen lassen sich auch per Umgebungsvariable setzen (siehe --help).
"""

from __future__ import annotations

import argparse
import base64
import json
import logging
import math
import os
import random
import signal
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timezone

log = logging.getLogger("apache-loggen-opensearch")

# ---------------------------------------------------------------------------
# Stammdaten
# ---------------------------------------------------------------------------

BROWSER_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.0 Safari/605.1.15",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64; rv:144.0) Gecko/20100101 Firefox/144.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 26_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (Linux; Android 15; SM-S928B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 16; Pixel 9) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (iPad; CPU OS 26_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/26.0 Mobile/15E148 Safari/604.1",
]
BOT_UAS = [
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
    "Mozilla/5.0 (compatible; AhrefsBot/7.0; +http://ahrefs.com/robot/)",
    "Mozilla/5.0 (compatible; SemrushBot/7~bl; +http://www.semrush.com/bot.html)",
    "Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)",
    "Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko; compatible; GPTBot/1.2; +https://openai.com/gptbot)",
]
API_UAS = [
    "python-requests/2.32.5",
    "curl/8.14.1",
    "Go-http-client/2.0",
    "okhttp/4.12.0",
    "Apache-HttpClient/5.4.1 (Java/21.0.8)",
    "ShopSync/3.4.1",
]
SCANNER_UAS = [
    "Mozilla/5.0 zgrab/0.x",
    "Mozilla/5.0 (compatible; Nmap Scripting Engine; https://nmap.org/book/nse.html)",
    "masscan/1.3 (https://github.com/robertdavidgraham/masscan)",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "-",
]
API_USERS = ["svc-shop", "svc-erp", "svc-warehouse", "partner-acme", "partner-globex"]
EXTERNAL_REFERERS = [
    "https://www.google.com/", "https://www.google.de/", "https://www.bing.com/",
    "https://duckduckgo.com/", "https://www.ecosia.org/", "https://t.co/",
    "https://www.facebook.com/", "https://www.linkedin.com/",
]
BLOG_SLUGS = ["sommer-sale-2026", "neues-lager", "nachhaltigkeit", "release-notes-4-2",
              "tipps-fuer-den-winter", "interview-team", "faq-versand", "jobs"]
IMAGES = ["hero", "team", "logo", "banner-herbst", "produkt-01", "produkt-02", "produkt-03", "icon-sprite"]
SEARCH_TERMS = ["laptop", "monitor", "tastatur", "maus", "headset", "usb-c+kabel", "ssd", "drucker", "webcam"]
SCANNER_PATHS = ["/wp-login.php", "/.env", "/.git/config", "/phpmyadmin/", "/xmlrpc.php",
                 "/admin", "/wp-admin/setup-config.php", "/vendor/phpunit/phpunit/src/Util/PHP/eval-stdin.php",
                 "/cgi-bin/luci", "/server-status", "/config.json", "/.aws/credentials", "/actuator/health"]

CT_HTML = "text/html; charset=UTF-8"
CT_JSON = "application/json"
CT_CSS = "text/css"
CT_JS = "application/javascript"
CT_JPG = "image/jpeg"
CT_PNG = "image/png"
CT_ICO = "image/x-icon"
CT_TXT = "text/plain"
CT_XML = "application/xml"


@dataclass
class Route:
    """Eine Ressource einer Website."""
    path: str                    # Pfad, {x} wird zur Laufzeit ersetzt
    weight: float                # relative Häufigkeit
    methods: tuple               # ((Methode, Gewicht), ...)
    ctype: str                   # Content-Type der Antwort
    size: tuple                  # (min, max) Body-Bytes
    lat_ms: float                # Median-Latenz in Millisekunden
    kind: str = "page"           # page | static | api | form
    auth: bool = False           # erfordert Login/Session bzw. Basic-Auth
    query: tuple = ()            # mögliche Query-Strings


def _site_routes() -> dict[str, list[Route]]:
    GET = (("GET", 1.0),)
    static = [
        Route("/static/css/main.{v}.css", 6, GET, CT_CSS, (18_000, 64_000), 0.6, "static"),
        Route("/static/js/app.{v}.js", 6, GET + (("HEAD", 0.02),), CT_JS, (90_000, 420_000), 0.8, "static"),
        Route("/static/img/{img}.jpg", 9, GET, CT_JPG, (25_000, 680_000), 1.2, "static"),
        Route("/static/img/{img}.png", 4, GET, CT_PNG, (2_000, 90_000), 0.9, "static"),
        Route("/favicon.ico", 3, GET, CT_ICO, (1_150, 15_086), 0.4, "static"),
    ]
    www = [
        Route("/", 10, GET + (("HEAD", 0.03),), CT_HTML, (24_000, 48_000), 35, "page"),
        Route("/ueber-uns", 2, GET, CT_HTML, (18_000, 30_000), 28, "page"),
        Route("/kontakt", 2, (("GET", 0.8), ("POST", 0.2)), CT_HTML, (12_000, 20_000), 45, "form"),
        Route("/blog", 4, GET, CT_HTML, (30_000, 70_000), 60, "page", query=("?page=2", "?page=3", "?tag=news")),
        Route("/blog/{slug}", 6, GET, CT_HTML, (35_000, 90_000), 55, "page",
              query=("?utm_source=newsletter&utm_medium=email", "?utm_source=linkedin")),
        Route("/impressum", 1, GET, CT_HTML, (9_000, 12_000), 20, "page"),
        Route("/datenschutz", 1, GET, CT_HTML, (30_000, 40_000), 22, "page"),
        Route("/robots.txt", 1.5, GET, CT_TXT, (120, 400), 0.5, "static"),
        Route("/sitemap.xml", 1, GET, CT_XML, (8_000, 40_000), 12, "static"),
    ] + static
    shop = [
        Route("/", 8, GET, CT_HTML, (40_000, 90_000), 70, "page"),
        Route("/produkte", 6, GET, CT_HTML, (60_000, 150_000), 110, "page",
              query=("?kategorie=notebooks", "?kategorie=zubehoer", "?sort=preis_asc", "?page=2")),
        Route("/produkte/{pid}", 10, GET, CT_HTML, (45_000, 120_000), 85, "page"),
        Route("/suche", 5, GET, CT_HTML, (30_000, 110_000), 180, "page", query=("?q={term}",)),
        Route("/warenkorb", 3, GET, CT_HTML, (20_000, 45_000), 60, "page"),
        Route("/warenkorb/add", 3, (("POST", 1.0),), CT_JSON, (80, 600), 75, "form"),
        Route("/checkout", 1.2, (("GET", 0.6), ("POST", 0.4)), CT_HTML, (25_000, 50_000), 260, "form", auth=True),
        Route("/login", 2, (("GET", 0.6), ("POST", 0.4)), CT_HTML, (10_000, 18_000), 90, "form"),
        Route("/konto", 1.5, GET, CT_HTML, (15_000, 35_000), 95, "page", auth=True),
        Route("/api/cart/count", 4, GET, CT_JSON, (20, 60), 8, "api"),
    ] + static
    api = [
        Route("/api/v1/products", 8, GET, CT_JSON, (4_000, 120_000), 45, "api", True,
              query=("?limit=50", "?limit=50&offset=50", "?limit=100&offset=200", "?updated_since=2026-10-01")),
        Route("/api/v1/products/{pid}", 10, (("GET", 0.85), ("PUT", 0.12), ("DELETE", 0.03)),
              CT_JSON, (600, 3_500), 18, "api", True),
        Route("/api/v1/orders", 6, (("GET", 0.6), ("POST", 0.4)), CT_JSON, (800, 60_000), 70, "api", True,
              query=("?status=open", "?status=shipped&limit=100")),
        Route("/api/v1/orders/{oid}", 5, (("GET", 0.8), ("PATCH", 0.2)), CT_JSON, (900, 4_000), 25, "api", True),
        Route("/api/v1/stock/{pid}", 6, GET, CT_JSON, (60, 200), 6, "api", True),
        Route("/api/v1/users/me", 2, GET, CT_JSON, (300, 900), 12, "api", True),
        Route("/api/v1/auth/token", 2, (("POST", 1.0),), CT_JSON, (350, 700), 140, "api"),
        Route("/api/v1/health", 4, (("GET", 1.0),), CT_JSON, (15, 40), 1.5, "api"),
        Route("/api/v1/openapi.json", 0.5, GET, CT_JSON, (85_000, 95_000), 9, "static"),
    ]
    return {"www": www, "shop": shop, "api": api}


# ---------------------------------------------------------------------------
# Clients
# ---------------------------------------------------------------------------

@dataclass
class Client:
    kind: str                         # browser | bot | api | scanner
    ip: str
    ua: str
    xff: str = "-"                    # interne IP hinter einem Proxy
    session: str | None = None        # JSESSIONID
    api_user: str = "-"
    last_page: dict = field(default_factory=dict)   # vhost -> letzte Seite (für Referer)


def _rand_public_ip(rnd: random.Random) -> str:
    while True:
        a = rnd.choice([2, 5, 31, 37, 46, 62, 77, 78, 79, 80, 84, 85, 87, 88, 89, 91, 92, 93, 94,
                        95, 109, 134, 141, 145, 176, 178, 185, 188, 193, 194, 195, 212, 213, 217])
        return f"{a}.{rnd.randint(0, 255)}.{rnd.randint(0, 255)}.{rnd.randint(1, 254)}"


def _session_id(rnd: random.Random) -> str:
    return "%032X.node%d" % (rnd.getrandbits(128), rnd.randint(1, 3))


def build_clients(rnd: random.Random, n: int = 400) -> tuple[list[Client], list[float]]:
    clients, weights = [], []
    proxy_ips = [_rand_public_ip(rnd) for _ in range(4)]       # Firmen-Proxys
    for i in range(n):
        r = rnd.random()
        if r < 0.80:
            c = Client("browser", _rand_public_ip(rnd), rnd.choice(BROWSER_UAS))
            if rnd.random() < 0.12:                              # hinter Proxy
                c.xff = f"192.168.{rnd.randint(0, 40)}.{rnd.randint(2, 254)}"
                c.ip = rnd.choice(proxy_ips)
            if rnd.random() < 0.7:
                c.session = _session_id(rnd)
        elif r < 0.90:
            c = Client("bot", _rand_public_ip(rnd), rnd.choice(BOT_UAS))
        elif r < 0.97:
            c = Client("api", f"10.10.{rnd.randint(1, 9)}.{rnd.randint(10, 250)}",
                       rnd.choice(API_UAS), api_user=rnd.choice(API_USERS))
        else:
            c = Client("scanner", _rand_public_ip(rnd), rnd.choice(SCANNER_UAS))
        clients.append(c)
        # Zipf-ähnliche Verteilung: wenige Clients erzeugen viel Traffic
        weights.append(1.0 / (i + 1) ** 0.9)
    rnd.shuffle(weights)
    return clients, weights


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------

class ApacheLogGenerator:
    def __init__(self, domain: str, seed: int | None = None, add_msg: bool = True):
        self.rnd = random.Random(seed)
        self.add_msg = add_msg
        rnd = self.rnd
        routes = _site_routes()
        self.sites = {
            f"www.{domain}":  {"routes": routes["www"],  "ip": "10.0.1.11", "weight": 0.45},
            f"shop.{domain}": {"routes": routes["shop"], "ip": "10.0.1.12", "weight": 0.35},
            f"api.{domain}":  {"routes": routes["api"],  "ip": "10.0.1.13", "weight": 0.20},
        }
        self.vhosts = list(self.sites)
        self.clients, self.client_weights = build_clients(rnd)
        self.by_kind: dict[str, tuple[list[Client], list[float]]] = {}
        for c, w in zip(self.clients, self.client_weights):
            lst = self.by_kind.setdefault(c.kind, ([], []))
            lst[0].append(c)
            lst[1].append(w)
        self.asset_version = "%08x" % rnd.getrandbits(32)
        self.pids = [rnd.randint(2_000, 60_000) for _ in range(8)]
        self.pid_counter = 0
        # Störfall-Simulation
        self.incident_until = 0.0
        self.incident_vhost: str | None = None
        self.incident_factor = 1.0
        self.next_incident = 0.0

    # -- Hilfen ------------------------------------------------------------
    def _wchoice(self, items, weights):
        return self.rnd.choices(items, weights=weights, k=1)[0]

    def _pid(self) -> int:
        # Worker werden nach einigen tausend Requests recycelt (MaxConnectionsPerChild)
        self.pid_counter += 1
        if self.pid_counter % 4000 == 0:
            self.pids[self.rnd.randrange(len(self.pids))] = self.rnd.randint(2_000, 60_000)
        return self.rnd.choice(self.pids)

    def _latency_us(self, median_ms: float, factor: float = 1.0) -> int:
        # Lognormal-verteilt, sigma ~0.6 -> realistischer langer Schwanz
        val = self.rnd.lognormvariate(math.log(median_ms * 1000.0), 0.6) * factor
        return max(80, int(val))

    def _update_incident(self, now: float) -> None:
        if self.next_incident == 0.0:
            self.next_incident = now + self.rnd.uniform(300, 1200)
        if now >= self.next_incident and now >= self.incident_until:
            self.incident_vhost = self.rnd.choice(self.vhosts)
            self.incident_until = now + self.rnd.uniform(45, 240)
            self.incident_factor = self.rnd.uniform(3, 9)
            self.next_incident = self.incident_until + self.rnd.uniform(600, 2400)
            log.debug("Störfall auf %s bis %s", self.incident_vhost, time.ctime(self.incident_until))

    def _fill(self, path: str) -> str:
        r = self.rnd
        return (path.replace("{v}", self.asset_version[:8])
                    .replace("{img}", r.choice(IMAGES))
                    .replace("{slug}", r.choice(BLOG_SLUGS))
                    .replace("{pid}", str(r.randint(10_000, 10_400)))
                    .replace("{oid}", str(r.randint(2_600_000, 2_699_999)))
                    .replace("{term}", r.choice(SEARCH_TERMS)))

    # -- ein Eintrag -------------------------------------------------------
    def make_entry(self, ts: float) -> dict:
        r = self.rnd
        self._update_incident(ts)

        # Client wählen
        kind = self._wchoice(["browser", "bot", "api", "scanner"], [0.70, 0.12, 0.15, 0.03])
        pool, weights = self.by_kind.get(kind) or (self.clients, self.client_weights)
        client = self._wchoice(pool, weights)

        # vhost + Route
        if kind == "api":
            vhost = self.vhosts[2] if r.random() < 0.9 else self.vhosts[1]
        else:
            vhost = self._wchoice(self.vhosts, [s["weight"] for s in self.sites.values()])
        site = self.sites[vhost]

        scanner_hit = kind == "scanner" or (kind != "api" and r.random() < 0.004)
        if scanner_hit:
            route = Route(r.choice(SCANNER_PATHS), 1, (("GET", 0.85), ("POST", 0.1), ("HEAD", 0.05)),
                          CT_HTML, (180, 320), 2.5, "scan")
        else:
            routes = site["routes"]
            if kind == "api":
                routes = [x for x in routes if x.kind in ("api", "static")] or routes
            elif kind == "bot":
                routes = [x for x in routes if x.kind in ("page", "static")] or routes
            route = self._wchoice(routes, [x.weight for x in routes])

        method = self._wchoice([m for m, _ in route.methods], [w for _, w in route.methods])
        path = self._fill(route.path)
        query = self._fill(r.choice(route.query)) if route.query and r.random() < 0.35 else ""

        protocol = "HTTP/2.0" if (kind == "browser" and r.random() < 0.55) else "HTTP/1.1"
        if kind == "scanner" and r.random() < 0.3:
            protocol = "HTTP/1.0"
        port = 443
        if kind in ("bot", "scanner") and r.random() < 0.15 or r.random() < 0.03:
            port, protocol = 80, ("HTTP/1.0" if protocol == "HTTP/1.0" else "HTTP/1.1")

        incident = (self.incident_vhost == vhost and ts < self.incident_until)
        lat_factor = self.incident_factor if incident else 1.0

        # --- Statuscode ---------------------------------------------------
        status, location = 200, "-"
        if port == 80:
            status, location = 301, f"https://{vhost}{path}{query}"
        elif route.kind == "scan":
            status = r.choices([404, 403, 401, 400, 405], [0.80, 0.08, 0.05, 0.04, 0.03])[0]
            if path == "/admin":
                status = 401
        else:
            if method == "POST":
                status = 201 if route.kind == "api" and "orders" in path else 200
                if route.path in ("/login", "/checkout", "/kontakt") and r.random() < 0.75:
                    status, location = 302, f"https://{vhost}" + {"/login": "/konto", "/checkout": "/checkout/danke",
                                                                     "/kontakt": "/kontakt?gesendet=1"}[route.path]
            elif method == "DELETE":
                status = 204
            if route.auth and kind == "browser" and not client.session:
                status, location = 302, f"https://{vhost}/login?next={urllib.parse.quote(path)}"
            elif route.auth and kind == "api" and r.random() < 0.02:
                status = 401
            if status in (200, 201):
                x = r.random()
                if route.kind == "static" and method == "GET" and kind == "browser" and x < 0.25:
                    status = 304
                elif "{pid}" in route.path and x < 0.03:
                    status = 404
                elif method in ("POST", "PUT", "PATCH") and x < 0.04:
                    status = r.choice([400, 422, 409])
                elif kind == "api" and x < 0.01:
                    status = 429
                elif x < 0.006:
                    status = r.choice([403, 404, 405, 408])
            # Serverfehler: Grundrauschen + Störfall
            p5xx = 0.006 if route.kind != "static" else 0.001
            if incident:
                p5xx = 0.25 if route.kind != "static" else 0.04
            if r.random() < p5xx:
                status = r.choices([500, 502, 503, 504], [0.45, 0.2, 0.25, 0.1])[0]
                location = "-"

        # --- Dauer ----------------------------------------------------------
        if status == 301:
            dur = self._latency_us(0.25)
        elif status == 304:
            dur = self._latency_us(0.3)
        elif status == 504:
            dur = int(r.uniform(30, 60) * 1_000_000)
        elif status in (502, 503):
            dur = self._latency_us(2.0)
        elif status in (401, 403, 404, 405, 429) and route.kind in ("scan", "static"):
            dur = self._latency_us(1.5)
        else:
            dur = self._latency_us(route.lat_ms, lat_factor if route.kind != "static" else 1.0)

        # --- Größen --------------------------------------------------------
        hdr_out = r.randint(220, 480)
        if method == "HEAD" or status in (204, 301, 302, 304):
            body_out = 0 if status != 301 else r.randint(220, 260)
        elif status >= 500:
            body_out = r.randint(300, 650)
        elif status >= 400:
            body_out = r.randint(150, 1_200) if route.ctype == CT_JSON else r.randint(196, 9_000)
        else:
            body_out = r.randint(*route.size)
        bytes_out = hdr_out + body_out

        req_line = len(method) + len(path) + len(query) + len(protocol) + 4
        bytes_in = req_line + r.randint(180, 750) + (len(client.ua) if client.ua != "-" else 0)
        if client.session and kind == "browser" and vhost == self.vhosts[1]:
            bytes_in += 50
        if method in ("POST", "PUT", "PATCH"):
            bytes_in += r.randint(120, 6_000)

        # --- Content-Type --------------------------------------------------
        if status in (204, 304):
            ctype = "-"
        elif status in (301, 302):
            ctype = "text/html; charset=iso-8859-1"
        elif status >= 400 and route.ctype != CT_JSON:
            ctype = "text/html; charset=iso-8859-1"
        else:
            ctype = route.ctype

        # --- Referer -------------------------------------------------------
        referer = "-"
        if kind == "browser":
            if route.kind == "static":
                referer = f"https://{vhost}{client.last_page.get(vhost, '/')}"
            elif vhost in client.last_page and r.random() < 0.75:
                referer = f"https://{vhost}{client.last_page[vhost]}"
            elif r.random() < 0.5:
                referer = r.choice(EXTERNAL_REFERERS)
            if route.kind in ("page", "form") and method == "GET" and status == 200:
                client.last_page[vhost] = path

        # --- Session / User ------------------------------------------------
        if client.session and r.random() < 0.002:        # Session läuft ab / neu
            client.session = _session_id(r)
        jsession = client.session if (client.session and vhost == self.vhosts[1] and kind == "browser") else "-"
        user = client.api_user if (kind == "api" and route.auth and status != 401) else "-"

        # --- keepalive (%X) ------------------------------------------------
        if status == 504 or (dur > 5_000_000 and r.random() < 0.3) or r.random() < 0.01:
            keepalive = "X"
        elif protocol == "HTTP/1.0" or r.random() < 0.12:
            keepalive = "-"
        else:
            keepalive = "+"

        server_port = str(port)
        entry = {
            "@timestamp": datetime.fromtimestamp(ts).astimezone().strftime("%Y-%m-%dT%H:%M:%S%z"),
            "client_ip": client.ip,
            "client_port": str(r.randint(32768, 60999)),
            "server_ip": site["ip"],
            "X-Forwarded-For": client.xff,
            "user": user,
            "REMOTE_USER": "-",
            "JSESSIONID": jsession,
            "pid": str(self._pid()),
            "protocol": protocol,
            "http_method": method,
            "vhost": vhost,
            "service_port": server_port,
            "path": path,
            "query_string": query,
            "referer": referer,
            "user_agent": client.ua,
            "response_code": str(status),
            "response_location": location,
            "Content-Type": ctype,
            "bytes_in": str(bytes_in),
            "bytes_out": str(bytes_out),
            "keepalive": keepalive,
            "duration_micros": str(dur),
        }
        if self.add_msg:
            # Kurzzeile für die Logansicht in Grafana / VMUI (wird als _msg gespeichert)
            entry["_msg"] = f'{client.ip} "{method} {path}{query} {protocol}" {status} {bytes_out} {dur}us'
        return entry

    def batch_for_second(self, second_start: float, n: int) -> list[dict]:
        offsets = sorted(self.rnd.random() for _ in range(n))
        return [self.make_entry(second_start + o) for o in offsets]



# ---------------------------------------------------------------------------
# Versand an OpenSearch
# ---------------------------------------------------------------------------

# Feldtypen passend zum LogFormat. Apache liefert alle Werte als Strings;
# OpenSearch wandelt Zahlen-Strings in numerische Felder automatisch um (coerce).
INDEX_MAPPINGS = {
    "dynamic_templates": [
        {"strings_as_keyword": {"match_mapping_type": "string",
                                "mapping": {"type": "keyword", "ignore_above": 1024}}}
    ],
    "properties": {
        # Apache %{%FT%T%z}t liefert +0100 (ohne Doppelpunkt) - das ISO-Format
        # lehnt das ab, daher das Muster ausdrücklich zuerst angeben.
        "@timestamp": {"type": "date",
                       "format": "yyyy-MM-dd'T'HH:mm:ssZ||strict_date_optional_time||epoch_millis"},
        "client_ip": {"type": "ip"},
        "client_port": {"type": "integer"},
        "server_ip": {"type": "ip"},
        "X-Forwarded-For": {"type": "keyword"},
        "user": {"type": "keyword"},
        "REMOTE_USER": {"type": "keyword"},
        "JSESSIONID": {"type": "keyword"},
        "pid": {"type": "integer"},
        "protocol": {"type": "keyword"},
        "http_method": {"type": "keyword"},
        "vhost": {"type": "keyword"},
        "service_port": {"type": "integer"},
        "path": {"type": "keyword", "ignore_above": 2048},
        "query_string": {"type": "keyword", "ignore_above": 2048},
        "referer": {"type": "keyword", "ignore_above": 2048},
        "user_agent": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 1024}}},
        "response_code": {"type": "short"},
        "response_location": {"type": "keyword", "ignore_above": 2048},
        "Content-Type": {"type": "keyword"},
        "bytes_in": {"type": "long"},
        "bytes_out": {"type": "long"},
        "keepalive": {"type": "keyword"},
        "duration_micros": {"type": "long"},
        "message": {"type": "text"},
    },
}


class OpenSearchSender:
    def __init__(self, base_url: str, index: str, user: str | None, password: str | None,
                 verify_tls: bool, ca_cert: str | None, timeout: float, max_buffer: int,
                 template_name: str, shards: int, replicas: int):
        self.base = base_url.rstrip("/")
        self.index = index
        self.timeout = timeout
        self.max_buffer = max_buffer
        self.template_name = template_name
        self.shards, self.replicas = shards, replicas
        self.headers = {"Content-Type": "application/json"}
        if user:
            token = base64.b64encode(f"{user}:{password or ''}".encode()).decode()
            self.headers["Authorization"] = "Basic " + token
        self.ssl_ctx = None
        if self.base.startswith("https://"):
            if not verify_tls:
                self.ssl_ctx = ssl._create_unverified_context()
            else:
                self.ssl_ctx = ssl.create_default_context(cafile=ca_cert) if ca_cert else ssl.create_default_context()
        # Kein Proxy für den Weg zu OpenSearch (meist im lokalen Netz)
        handlers = [urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=self.ssl_ctx)] \
            if self.ssl_ctx else [urllib.request.ProxyHandler({})]
        self.opener = urllib.request.build_opener(*handlers)

        self.buffer: list[tuple[str, str]] = []      # (Indexname, Dokument-JSON)
        self.sent = 0
        self.failed = 0
        self.dropped = 0
        self.failed_since: float | None = None
        self._last_item_error_log = 0.0

    # -- HTTP --------------------------------------------------------------
    def _request(self, method: str, path: str, body: bytes | None = None,
                 content_type: str | None = None) -> tuple[int, dict]:
        headers = dict(self.headers)
        if content_type:
            headers["Content-Type"] = content_type
        req = urllib.request.Request(self.base + path, data=body, method=method, headers=headers)
        try:
            with self.opener.open(req, timeout=self.timeout) as resp:
                raw = resp.read()
                return resp.status, (json.loads(raw) if raw else {})
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                return e.code, json.loads(raw)
            except ValueError:
                return e.code, {"error": raw.decode("utf-8", "replace")[:500]}

    def index_for(self, doc: dict) -> str:
        if "%" not in self.index:
            return self.index
        # Tagesindex nach UTC-Datum des Eintrags (OpenSearch-Konvention)
        ts = datetime.strptime(doc["@timestamp"], "%Y-%m-%dT%H:%M:%S%z").astimezone(timezone.utc)
        return ts.strftime(self.index)

    def check_connection(self) -> bool:
        try:
            status, info = self._request("GET", "/")
        except (urllib.error.URLError, OSError) as e:
            log.warning("OpenSearch nicht erreichbar (%s) - versuche es im Betrieb weiter.", e)
            return False
        if status == 401:
            log.error("Anmeldung an OpenSearch fehlgeschlagen (401) - Benutzer/Passwort prüfen.")
            return False
        if status >= 400:
            log.warning("OpenSearch antwortet mit HTTP %d: %s", status, info)
            return False
        ver = info.get("version", {})
        log.info("Verbunden mit %s %s (Cluster '%s')",
                 ver.get("distribution", "opensearch"), ver.get("number", "?"), info.get("cluster_name", "?"))
        return True

    def ensure_template(self) -> None:
        prefix = self.index.split("%", 1)[0]
        pattern = (prefix + "*") if "%" in self.index else self.index
        body = {
            "index_patterns": [pattern],
            "priority": 100,
            "template": {
                "settings": {"number_of_shards": self.shards, "number_of_replicas": self.replicas,
                             "refresh_interval": "5s"},
                "mappings": INDEX_MAPPINGS,
            },
            "_meta": {"created_by": "apache_loggen_opensearch.py"},
        }
        try:
            status, resp = self._request("PUT", f"/_index_template/{urllib.parse.quote(self.template_name)}",
                                         json.dumps(body).encode())
        except (urllib.error.URLError, OSError) as e:
            log.warning("Index-Template konnte nicht angelegt werden (%s).", e)
            return
        if status < 300:
            log.info("Index-Template '%s' für '%s' angelegt/aktualisiert.", self.template_name, pattern)
        else:
            log.warning("Index-Template nicht angelegt (HTTP %d): %s - Felder werden dann dynamisch "
                        "erkannt (Zahlen als Text!).", status, json.dumps(resp)[:300])

    # -- Bulk ----------------------------------------------------------------
    def send(self, docs: list[dict], max_per_request: int = 5_000) -> bool:
        for d in docs:
            self.buffer.append((self.index_for(d), json.dumps(d, ensure_ascii=False, separators=(",", ":"))))
        if len(self.buffer) > self.max_buffer:
            drop = len(self.buffer) - self.max_buffer
            del self.buffer[:drop]
            self.dropped += drop
        ok = True
        while self.buffer and ok:
            chunk = self.buffer[:max_per_request]
            ok = self._bulk(chunk)
        return ok

    def _bulk(self, chunk: list[tuple[str, str]]) -> bool:
        lines = []
        for idx, doc in chunk:
            lines.append('{"index":{"_index":%s}}' % json.dumps(idx))
            lines.append(doc)
        body = ("\n".join(lines) + "\n").encode("utf-8")
        try:
            status, resp = self._request("POST", "/_bulk", body, "application/x-ndjson")
        except (urllib.error.URLError, OSError) as e:
            return self._fail(f"Verbindungsfehler: {e}")

        if status in (429, 502, 503, 504):
            return self._fail(f"OpenSearch überlastet/nicht bereit (HTTP {status})")
        if status in (401, 403):
            return self._fail(f"Keine Berechtigung (HTTP {status}): {json.dumps(resp)[:200]}")
        if status >= 400:
            # Anfrage als Ganzes ungültig - Wiederholen hilft nicht
            log.error("Bulk-Anfrage abgelehnt (HTTP %d): %s - %d Einträge verworfen.",
                      status, json.dumps(resp)[:300], len(chunk))
            del self.buffer[:len(chunk)]
            self.failed += len(chunk)
            return True

        retry: list[tuple[str, str]] = []
        chunk_failed = 0
        if resp.get("errors"):
            first_reason = None
            for item, entry in zip(resp.get("items", []), chunk):
                r = item.get("index") or item.get("create") or {}
                st = r.get("status", 200)
                if st == 429:
                    retry.append(entry)                      # später erneut versuchen
                elif st >= 300:
                    chunk_failed += 1
                    first_reason = first_reason or r.get("error")
            if first_reason and time.time() - self._last_item_error_log > 60:
                self._last_item_error_log = time.time()
                log.warning("Einzelne Einträge abgelehnt, z.B.: %s", json.dumps(first_reason)[:300])

        del self.buffer[:len(chunk)]
        self.failed += chunk_failed
        self.sent += len(chunk) - len(retry) - chunk_failed
        if retry:
            self.buffer[:0] = retry
            return self._fail(f"{len(retry)} Einträge wegen Überlast (429) zurückgestellt")
        if self.failed_since is not None:
            log.info("Verbindung zu OpenSearch wieder hergestellt.")
            self.failed_since = None
        return True

    def _fail(self, reason: str) -> bool:
        if self.failed_since is None:
            self.failed_since = time.time()
            log.warning("%s - puffere bis zu %d Einträge und versuche es weiter.", reason, self.max_buffer)
        return False


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def env(name: str, default):
    return os.environ.get(name, default)


def env_bool(name: str) -> bool:
    return env(name, "").strip().lower() in ("1", "true", "yes", "ja")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Apache-JSON-Log-Generator für OpenSearch",
                                formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--url", default=env("OS_URL", "https://localhost:9200"),
                   help="OpenSearch-URL (env OS_URL)")
    p.add_argument("--user", default=env("OS_USER", None), help="Benutzer (env OS_USER)")
    p.add_argument("--password", default=env("OS_PASSWORD", None),
                   help="Passwort (env OS_PASSWORD - besser als Parameter, erscheint nicht in 'ps')")
    p.add_argument("--insecure", action="store_true", default=env_bool("OS_INSECURE"),
                   help="TLS-Zertifikat nicht prüfen, z.B. bei Demo-Zertifikaten (env OS_INSECURE=1)")
    p.add_argument("--ca-cert", default=env("OS_CA_CERT", None),
                   help="CA-Zertifikat (PEM) zum Prüfen des Server-Zertifikats (env OS_CA_CERT)")
    p.add_argument("--index", default=env("OS_INDEX", "apache-logs-%Y.%m.%d"),
                   help="Indexname, strftime-Platzhalter erlaubt (env OS_INDEX)")
    p.add_argument("--template-name", default=env("OS_TEMPLATE_NAME", "apache-logs"),
                   help="Name des Index-Templates (env OS_TEMPLATE_NAME)")
    p.add_argument("--no-template", action="store_true", default=env_bool("OS_NO_TEMPLATE"),
                   help="kein Index-Template anlegen (env OS_NO_TEMPLATE=1)")
    p.add_argument("--shards", type=int, default=int(env("OS_SHARDS", 1)), help="Primär-Shards je Index")
    p.add_argument("--replicas", type=int, default=int(env("OS_REPLICAS", 0)),
                   help="Replikate je Index (0 für Single-Node, env OS_REPLICAS)")
    p.add_argument("--min-rate", type=int, default=int(env("LOGGEN_MIN_RATE", 2)),
                   help="min. Logs pro Sekunde (env LOGGEN_MIN_RATE)")
    p.add_argument("--max-rate", type=int, default=int(env("LOGGEN_MAX_RATE", 20)),
                   help="max. Logs pro Sekunde (env LOGGEN_MAX_RATE)")
    p.add_argument("--domain", default=env("LOGGEN_DOMAIN", "example.de"),
                   help="Domain für die vhosts www./shop./api. (env LOGGEN_DOMAIN)")
    p.add_argument("--flush-interval", type=float, default=float(env("LOGGEN_FLUSH_INTERVAL", 1)),
                   help="Sekunden zwischen zwei Bulk-Anfragen (env LOGGEN_FLUSH_INTERVAL)")
    p.add_argument("--timeout", type=float, default=float(env("LOGGEN_TIMEOUT", 10)), help="HTTP-Timeout in s")
    p.add_argument("--max-buffer", type=int, default=int(env("LOGGEN_MAX_BUFFER", 50_000)),
                   help="max. gepufferte Einträge bei Verbindungsfehlern")
    p.add_argument("--backfill", type=int, default=int(env("LOGGEN_BACKFILL", 0)),
                   help="vor dem Live-Betrieb N Minuten Historie erzeugen (env LOGGEN_BACKFILL)")
    p.add_argument("--duration", type=int, default=0, help="nach N Sekunden beenden (0 = endlos)")
    p.add_argument("--no-msg", action="store_true", default=env("LOGGEN_NO_MSG", "") == "1",
                   help="kein zusätzliches Feld 'message' erzeugen (env LOGGEN_NO_MSG=1)")
    p.add_argument("--stdout", action="store_true", help="Einträge zusätzlich auf stdout ausgeben")
    p.add_argument("--no-send", action="store_true", help="nichts an OpenSearch senden (Testmodus)")
    p.add_argument("--seed", type=int, default=None, help="Zufalls-Seed für reproduzierbare Daten")
    p.add_argument("-v", "--verbose", action="store_true", help="Debug-Ausgaben")
    a = p.parse_args()
    if not 1 <= a.min_rate <= a.max_rate:
        p.error("es muss gelten: 1 <= --min-rate <= --max-rate")
    if a.user and a.password is None:
        p.error("--user ohne Passwort: bitte OS_PASSWORD oder --password setzen")
    return a


def main() -> int:
    args = parse_args()
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)

    gen = ApacheLogGenerator(args.domain, seed=args.seed, add_msg=not args.no_msg)
    sender = None
    if not args.no_send:
        sender = OpenSearchSender(args.url, args.index, args.user, args.password,
                                  not args.insecure, args.ca_cert, args.timeout, args.max_buffer,
                                  args.template_name, args.shards, args.replicas)

    running = True

    def stop(signum, _frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    def prepare(entries: list[dict]) -> list[dict]:
        # Kurzzeile heißt in OpenSearch üblicherweise "message" (bei VictoriaLogs "_msg")
        for e in entries:
            if "_msg" in e:
                e["message"] = e.pop("_msg")
        return entries

    def emit(entries: list[dict]) -> None:
        entries = prepare(entries)
        if args.stdout:
            try:
                sys.stdout.write("".join(json.dumps(e, ensure_ascii=False, separators=(",", ":")) + "\n"
                                         for e in entries))
                sys.stdout.flush()
            except BrokenPipeError:          # z.B. bei "| head"
                stop(None, None)
                return
        if sender:
            sender.send(entries)

    log.info("Starte Generator: %d-%d Logs/s, vhosts=%s, Ziel=%s",
             args.min_rate, args.max_rate, ", ".join(gen.vhosts),
             "(kein Versand)" if args.no_send else f"{args.url} Index {args.index}")
    if sender:
        if args.insecure:
            log.warning("TLS-Zertifikatsprüfung ist abgeschaltet (--insecure).")
        if sender.check_connection() and not args.no_template:
            sender.ensure_template()

    # --- optional: Historie erzeugen ------------------------------------
    if args.backfill > 0:
        start = math.floor(time.time()) - args.backfill * 60
        end = math.floor(time.time())
        log.info("Erzeuge Historie für %d Minuten ...", args.backfill)
        chunk: list[dict] = []
        sec = start
        while sec < end and running:
            chunk.extend(gen.batch_for_second(sec, gen.rnd.randint(args.min_rate, args.max_rate)))
            sec += 1
            if len(chunk) >= 5_000:
                emit(chunk)
                chunk = []
        if chunk:
            emit(chunk)
        log.info("Historie fertig (%d Einträge gesendet).", sender.sent if sender else 0)

    # --- Live-Betrieb ------------------------------------------------------
    t_start = time.time()
    next_tick = math.floor(t_start) + 1
    pending: list[dict] = []
    last_flush = time.time()
    stat_count, stat_time = 0, time.time()
    while running:
        sleep_for = next_tick - time.time()
        if sleep_for > 0:
            time.sleep(sleep_for)
        n = gen.rnd.randint(args.min_rate, args.max_rate)
        pending.extend(gen.batch_for_second(next_tick - 1, n))
        stat_count += n
        next_tick += 1
        if time.time() - next_tick > 5:          # nach Suspend nicht nachholen
            next_tick = math.floor(time.time()) + 1

        if time.time() - last_flush >= args.flush_interval:
            emit(pending)
            pending = []
            last_flush = time.time()

        if time.time() - stat_time >= 60:
            msg = "%d Logs in den letzten 60 s erzeugt" % stat_count
            if sender:
                msg += ", gesendet gesamt: %d, im Puffer: %d" % (sender.sent, len(sender.buffer))
                if sender.failed:
                    msg += ", abgelehnt: %d" % sender.failed
                if sender.dropped:
                    msg += ", verworfen (Puffer voll): %d" % sender.dropped
            log.info(msg)
            stat_count, stat_time = 0, time.time()

        if args.duration and time.time() - t_start >= args.duration:
            break

    if pending:
        emit(pending)
    if sender and sender.buffer:
        log.info("Sende Restpuffer (%d Einträge) ...", len(sender.buffer))
        sender.send([])
    log.info("Beendet.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
