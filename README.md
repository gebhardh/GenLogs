# Jupyter-Server & Apache-Log-Generator für VictoriaLogs

Dokumentation zu zwei Setups auf **Debian 13 (Trixie)**:

1. [Jupyter Notebook als systemd-Service](#1-jupyter-notebook-als-systemd-service): `setup-jupyter.sh`
2. [Apache-JSON-Log-Generator mit VictoriaLogs und Grafana](#2-apache-json-log-generator-für-victorialogs): `apache_loggen.py`, `install-loggen.sh`, `grafana-dashboard-apache-victorialogs.json`

---

## 1. Jupyter Notebook als systemd-Service

### Anforderung

Ein Script für Debian 13, das

- eine virtuelle Python-Umgebung anlegt,
- darin Jupyter installiert,
- ein Arbeitsverzeichnis anlegt,
- einen eigenen User anlegt und
- einen Service einrichtet, der Jupyter Notebook mit diesem User auf **Port 8880**, **IP 0.0.0.0** und **ohne Token** startet.

### Installation

```bash
sudo bash setup-jupyter.sh
```

Danach ist Jupyter unter `http://<server-ip>:8880/` erreichbar.

### Was das Script anlegt

| Element | Wert |
|---|---|
| User / Gruppe | `jupyter` (System-User, Passwort gesperrt, Home `/opt/jupyter`) |
| Virtuelle Umgebung | `/opt/jupyter/venv` mit dem Paket `notebook` (Jupyter Notebook 7) |
| Arbeitsverzeichnis | `/opt/jupyter/notebooks` |
| Konfiguration | `/opt/jupyter/jupyter_server_config.py` |
| systemd-Service | `jupyter-notebook` (aktiviert und gestartet) |

Pfade, User-Name und Port stehen oben im Script als Variablen und lassen sich anpassen. Das Script kann gefahrlos mehrfach laufen, z. B. um Jupyter zu aktualisieren.

### Erzeugte Jupyter-Konfiguration

```python
c.ServerApp.ip = '0.0.0.0'
c.ServerApp.port = 8880
c.ServerApp.port_retries = 0
c.ServerApp.open_browser = False
c.ServerApp.root_dir = '/opt/jupyter/notebooks'
c.ServerApp.allow_remote_access = True

# Authentifizierung deaktivieren (kein Token, kein Passwort)
c.IdentityProvider.token = ''
c.PasswordIdentityProvider.hashed_password = ''
c.PasswordIdentityProvider.password_required = False
```

### Betrieb

```bash
systemctl status jupyter-notebook
journalctl -u jupyter-notebook -f
/opt/jupyter/venv/bin/jupyter --version
```

### Sicherheitshinweis

Ohne Token kann jeder, der Port 8880 erreicht, beliebigen Code als User `jupyter` ausführen, auch über das Terminal im Browser. Den Service nur in einem vertrauenswürdigen Netz betreiben oder den Port per Firewall einschränken, z. B.:

```bash
ufw allow from 192.168.1.0/24 to any port 8880
```

### Troubleshooting: pip-Warnung bei der Installation

```
WARNING: Retrying (Retry(total=4, ...)) after connection broken by 'NewConnectionError(...
[Errno -2] Der Name oder der Dienst ist nicht bekannt')': /simple/pip/
```

pip konnte `pypi.org` beim ersten Versuch nicht per DNS auflösen und hat es automatisch erneut versucht. Ist die Installation danach durchgelaufen, ist die Warnung harmlos.

Typische Ursachen sind ein kurzer DNS-Aussetzer (oft direkt nach dem Booten), ein langsamer DNS-Server oder ein nicht antwortender erster Nameserver in `/etc/resolv.conf`.

Prüfen:

```bash
getent hosts pypi.org
cat /etc/resolv.conf
```

Tritt die Warnung bei jedem Lauf auf, einen zuverlässigen Nameserver eintragen (z. B. den Router oder `9.9.9.9`).

---

## 2. Apache-JSON-Log-Generator für VictoriaLogs

### Anforderung

- Generator für Apache-Access-Logs im JSON-Format (siehe [LogFormat](#logformat))
- Versand der Logs an **VictoriaLogs**
- **2 bis 20 Logs pro Sekunde** (zufällig)
- **Grafana-Dashboard** zur Auswertung

### Dateien

| Datei | Zweck |
|---|---|
| `apache_loggen.py` | Generator. Nur Python 3 (≥ 3.9) ohne Zusatzpakete. |
| `install-loggen.sh` | Installiert den Generator als systemd-Service `apache-loggen`. |
| `grafana-dashboard-apache-victorialogs.json` | Importierbares Grafana-Dashboard. |

### LogFormat

Der Generator erzeugt Einträge exakt in diesem Apache-Format:

```apache
LogFormat "{ \
 \"@timestamp\":\"%{%FT%T%z}t\", \
 \"client_ip\":\"%a\", \
 \"client_port\":\"%{remote}p\", \
 \"server_ip\":\"%A\", \
 \"X-Forwarded-For\":\"%{X-Forwarded-For}i\", \
 \"user\":\"%u\", \
 \"REMOTE_USER\":\"%{REMOTE_USER}i\", \
 \"JSESSIONID\":\"%{JSESSIONID}C\", \
 \"pid\":\"%p\", \
 \"protocol\":\"%H\", \
 \"http_method\":\"%m\", \
 \"vhost\":\"%{Host}i\", \
 \"service_port\":\"%p\", \
 \"path\":\"%U\", \
 \"query_string\":\"%q\", \
 \"referer\":\"%{Referer}i\", \
 \"user_agent\":\"%{User-agent}i\", \
 \"response_code\":\"%>s\", \
 \"response_location\":\"%{Location}o\", \
 \"Content-Type\":\"%{Content-Type}o\", \
 \"bytes_in\":\"%I\", \
 \"bytes_out\":\"%O\", \
 \"keepalive\":\"%X\", \
 \"duration_micros\":\"%D\", \
 }" logstash_json
```

> **Hinweise zum Original-LogFormat**
> - `pid` verwendet `%p`, das ist der **Server-Port**. Die Prozess-ID wäre **`%P`**. Der Generator schreibt realistische Worker-PIDs; in der echten Apache-Konfiguration sollte `%P` stehen.
> - Hinter `duration_micros` steht ein **Komma vor `}`**. Damit ist die Ausgabe des echten Apache kein gültiges JSON. Das Komma sollte entfernt werden.

### Beispiel-Eintrag

```json
{"@timestamp":"2026-10-26T20:20:34+0100","client_ip":"84.136.211.22","client_port":"33456","server_ip":"10.0.1.12","X-Forwarded-For":"-","user":"-","REMOTE_USER":"-","JSESSIONID":"-","pid":"15668","protocol":"HTTP/2.0","http_method":"GET","vhost":"shop.example.de","service_port":"443","path":"/suche","query_string":"?q=webcam","referer":"-","user_agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:144.0) Gecko/20100101 Firefox/144.0","response_code":"200","response_location":"-","Content-Type":"text/html; charset=UTF-8","bytes_in":"766","bytes_out":"101720","keepalive":"+","duration_micros":"200394","_msg":"84.136.211.22 \"GET /suche?q=webcam HTTP/2.0\" 200 101720 200394us"}
```

### Simulierte Daten

- **Drei vhosts:** `www.<domain>` (Website), `shop.<domain>` (Shop mit Sessions/`JSESSIONID`), `api.<domain>` (REST-API mit Basic-Auth-Usern)
- **Clients:** ca. 400 Clients mit ungleicher Verteilung (wenige Clients erzeugen viel Traffic): Browser, Suchmaschinen-Bots, API-Clients, Scanner; ein Teil der Browser hinter Firmen-Proxys mit `X-Forwarded-For`
- **Statuscodes:** realistisch gemischt (200, 201, 204, 301 bei Port 80, 302 bei Login/Formularen, 304 bei Static-Assets, 4xx bei Scannern und fehlerhaften Requests, 5xx als Grundrauschen)
- **Antwortzeiten:** lognormal verteilt je Ressourcentyp (statisch, Seite, API, Checkout)
- **Störfälle:** alle 10 bis 40 Minuten auf einem zufälligen vhost für 1 bis 4 Minuten mit vielen 5xx-Fehlern und erhöhten Antwortzeiten, damit im Dashboard auch Auffälligkeiten sichtbar werden

### Installation als Service (Debian 13)

Im Verzeichnis mit `apache_loggen.py`:

```bash
sudo bash install-loggen.sh http://<victorialogs-host>:9428
```

Das Script

- installiert `python3`,
- legt den System-User `loggen` an,
- kopiert den Generator nach `/opt/apache-loggen/`,
- legt die Einstellungen in `/etc/default/apache-loggen` an (bestehende Datei bleibt unverändert),
- richtet den gehärteten systemd-Service `apache-loggen` ein und startet ihn.

Betrieb:

```bash
systemctl status apache-loggen
journalctl -u apache-loggen -f
sudo nano /etc/default/apache-loggen && sudo systemctl restart apache-loggen
```

### Konfiguration

Alle Optionen gehen als Kommandozeilenparameter oder Umgebungsvariable (im Service über `/etc/default/apache-loggen`).

| Parameter | Umgebungsvariable | Standard | Bedeutung |
|---|---|---|---|
| `--url` | `VL_URL` | `http://localhost:9428` | Basis-URL von VictoriaLogs |
| `--min-rate` | `LOGGEN_MIN_RATE` | `2` | min. Logs pro Sekunde |
| `--max-rate` | `LOGGEN_MAX_RATE` | `20` | max. Logs pro Sekunde |
| `--domain` | `LOGGEN_DOMAIN` | `example.de` | Domain für `www.`, `shop.`, `api.` |
| `--stream-fields` | `LOGGEN_STREAM_FIELDS` | `vhost` | Felder, die in VictoriaLogs den Log-Stream bilden |
| `--backfill N` | `LOGGEN_BACKFILL` | `0` | vor dem Live-Betrieb N Minuten Historie erzeugen |
| `--no-msg` | `LOGGEN_NO_MSG=1` | aus | kein zusätzliches `_msg`-Feld |
| `--account-id` | `VL_ACCOUNT_ID` | – | Tenant AccountID (optional) |
| `--project-id` | `VL_PROJECT_ID` | – | Tenant ProjectID (optional) |
| `--timeout` | `LOGGEN_TIMEOUT` | `5` | HTTP-Timeout in Sekunden |
| `--max-buffer` | `LOGGEN_MAX_BUFFER` | `50000` | max. gepufferte Einträge bei Verbindungsfehlern |
| `--duration N` | – | `0` | nach N Sekunden beenden (0 = endlos) |
| `--stdout` | – | aus | Einträge zusätzlich auf stdout ausgeben |
| `--no-send` | – | aus | nichts senden (Testmodus) |
| `--seed N` | – | – | Zufalls-Seed für reproduzierbare Daten |
| `-v` | – | aus | Debug-Ausgaben |

> `LOGGEN_BACKFILL` wirkt bei **jedem** Neustart des Service. Für den Dauerbetrieb auf `0` lassen.

### Manuelle Nutzung

```bash
# nur auf der Konsole ausgeben, nichts senden
./apache_loggen.py --stdout --no-send

# an VictoriaLogs senden
./apache_loggen.py --url http://192.168.1.50:9428

# erst eine Stunde Historie erzeugen, dann live weiter
./apache_loggen.py --url http://192.168.1.50:9428 --backfill 60
```

Der Generator schreibt jede Minute eine Statuszeile (erzeugte, gesendete und gepufferte Einträge) nach stderr bzw. ins Journal. Ist VictoriaLogs nicht erreichbar, werden Einträge gepuffert und nachgesendet.

### Versand an VictoriaLogs

Gesendet wird gebündelt an den JSON-Lines-Endpunkt:

```
POST /insert/jsonline?_time_field=@timestamp&_stream_fields=vhost
Content-Type: application/stream+json
```

Wichtig zu wissen:

- **Content-Type ist Pflicht.** Ohne `Content-Type: application/stream+json` verwirft VictoriaLogs den Body stillschweigend und antwortet trotzdem mit HTTP 200. Bei eigenen Tests mit `curl` den Header unbedingt angeben:
  ```bash
  echo '{"_msg":"test"}' | curl -X POST -H 'Content-Type: application/stream+json' \
       --data-binary @- 'http://localhost:9428/insert/jsonline'
  ```
- **`@timestamp`** im Apache-Format (`2026-10-26T20:16:21+0100`, Zeitzone ohne Doppelpunkt) wird korrekt gelesen. VictoriaLogs übernimmt das Feld als `_time` und speichert es nicht zusätzlich unter dem alten Namen.
- **`_msg`:** Das Apache-Format hat kein Nachrichtenfeld. Der Generator fügt daher eine kurze Zeile hinzu (`IP "METHODE PFAD PROTOKOLL" STATUS BYTES DAUERus`), die in den Logansichten als Nachricht erscheint.
- **Leere Felder** (z. B. `query_string: ""`) speichert VictoriaLogs nicht.

Getestet mit VictoriaLogs **v1.53.0**.

---

## 3. Grafana-Dashboard

### Voraussetzungen

- Grafana mit dem Plugin **`victoriametrics-logs-datasource`** (VictoriaLogs-Datasource)
- eine eingerichtete VictoriaLogs-Datenquelle

### Import

1. *Dashboards → New → Import*
2. `grafana-dashboard-apache-victorialogs.json` hochladen
3. Oben im Dashboard die Datenquelle wählen (Variable `datasource`)

### Variablen

| Variable | Typ | Zweck |
|---|---|---|
| `datasource` | Datenquelle | Auswahl der VictoriaLogs-Instanz |
| `vhost` | Feldwerte von `vhost` | Mehrfachauswahl, inkl. "All" |
| `filter` | Ad-hoc-Filter | beliebige zusätzliche Feldfilter |

Alle Abfragen verwenden den Basisfilter `vhost:$vhost response_code:*`.

### Panels

**Überblick**
- Requests, Ø Requests/s, 5xx-Quote, 4xx-Quote, p95-Antwortzeit, ausgehender Traffic, Anzahl Client-IPs

**Traffic & Fehler**
- Requests/s nach Statusklasse (2xx/3xx/4xx/5xx, gestapelt)
- Requests/s nach vhost
- Fehlerquote 4xx/5xx in %
- 5xx-Fehler/s nach vhost

**Performance**
- Antwortzeit p50 / p95 / p99
- p95-Antwortzeit nach vhost
- Datendurchsatz ein-/ausgehend (Bytes/s)
- Langsamste Endpunkte (p95, mind. 30 Requests; numerische Pfadsegmente werden zu `{id}` zusammengefasst)

**Verteilungen**
- Statuscodes, HTTP-Methoden, Protokolle

**Top 10**
- Pfade, Client-IPs, 404-Pfade, User-Agents, Clients mit 5xx

**Logs**
- Fehler (Status ≥ 400)
- Alle Requests

### Beispiel-Abfragen (LogsQL)

```logsql
# Requests/s nach Statusklasse
vhost:$vhost response_code:* | math floor(response_code/100) as cls | stats by (cls) rate() rps

# Fehlerquote in %
vhost:$vhost response_code:*
  | stats count() total, count() if (response_code:>=500) e
  | math e*100/total as pct | fields pct

# Antwortzeit-Perzentile in ms
vhost:$vhost response_code:*
  | stats quantile(0.5, duration_micros) p50, quantile(0.95, duration_micros) p95, quantile(0.99, duration_micros) p99
  | math p50/1000 as p50, p95/1000 as p95, p99/1000 as p99

# Top 10 Pfade
vhost:$vhost response_code:* | stats by (path) count() hits | first 10 by (hits desc)
```

> Bei Zeitreihen mit `math … | fields …` muss `_time` in der Feldliste stehen (`| fields _time, pct_4xx, pct_5xx`), sonst gehen die Zeitbuckets verloren.

### Teststand

Alle Dashboard-Abfragen wurden direkt gegen VictoriaLogs v1.53.0 mit generierten Daten geprüft, sowohl mit "All" als auch mit einzelnen vhosts. Die Darstellung in Grafana selbst wurde nicht getestet.
