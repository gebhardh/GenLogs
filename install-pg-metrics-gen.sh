#!/usr/bin/env bash
#
# install-pg-metrics-gen.sh - PostgreSQL-Metrik-Generator (Telegraf-Format) als
#                                systemd-Service auf Debian/Ubuntu
#
#  - legt den System-User "metricsgen" an
#  - kopiert pg_metrics_gen.py nach /opt/pg-metrics-gen
#  - legt /etc/default/pg-metrics-gen mit den Einstellungen an
#  - richtet den systemd-Service "pg-metrics-gen" ein und startet ihn
#
# Aufruf (im Verzeichnis mit pg_metrics_gen.py):
#   sudo bash install-pg-metrics-gen.sh [VICTORIAMETRICS_URL]
#   z.B. sudo bash install-pg-metrics-gen.sh http://192.168.1.50:8428
#
set -euo pipefail

VM_URL_ARG="${1:-http://localhost:8428}"
SVC_USER="metricsgen"
INSTALL_DIR="/opt/pg-metrics-gen"
SCRIPT="pg_metrics_gen.py"
ENV_FILE="/etc/default/pg-metrics-gen"
SERVICE_NAME="pg-metrics-gen"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() { echo -e "\e[1;32m[+]\e[0m $*"; }
err() { echo -e "\e[1;31m[!]\e[0m $*" >&2; }

[[ $EUID -eq 0 ]] || { err "Bitte als root ausführen (sudo bash $0)."; exit 1; }
[[ -f "${SRC_DIR}/${SCRIPT}" ]] || { err "${SCRIPT} nicht gefunden in ${SRC_DIR}."; exit 1; }

log "Installiere python3 ..."
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq python3

if ! id "${SVC_USER}" &>/dev/null; then
    log "Lege System-User '${SVC_USER}' an ..."
    useradd --system --no-create-home --home-dir "${INSTALL_DIR}" \
            --shell /usr/sbin/nologin --user-group "${SVC_USER}"
fi

log "Kopiere Generator nach ${INSTALL_DIR} ..."
install -d -o root -g root -m 0755 "${INSTALL_DIR}"
install -o root -g root -m 0755 "${SRC_DIR}/${SCRIPT}" "${INSTALL_DIR}/${SCRIPT}"

if [[ -f "${ENV_FILE}" ]]; then
    log "${ENV_FILE} existiert bereits - bleibt unverändert."
else
    log "Schreibe Einstellungen nach ${ENV_FILE} ..."
    umask 077
    cat > "${ENV_FILE}" <<EOF
# Einstellungen für ${SERVICE_NAME} (nach Änderung: systemctl restart ${SERVICE_NAME})

# --- VictoriaMetrics -------------------------------------------------------
VM_URL=${VM_URL_ARG}
# Single-Node: /write   |   Cluster (vminsert): /insert/0/influx/write
VM_WRITE_PATH=/write
#VM_USER=
#VM_PASSWORD=
#VM_BEARER_TOKEN=

# --- Simulierte Server -----------------------------------------------------
# Erfassungsintervall in Sekunden (wie Telegraf "interval")
PMG_INTERVAL=10
PMG_HOSTS=4
PMG_DBS_PER_HOST=8
PMG_HOST_PREFIX=pg-
# PostgreSQL-Hauptversion (bestimmt die Spalten; ab 17 ohne Checkpoint-Werte)
PMG_PG_VERSION=16
# Tag "server" = bereinigte Verbindungsadresse aus der Telegraf-Konfiguration
PMG_SERVER_TAG="host=localhost user=telegraf dbname=postgres"
# Wie Telegraf ignored_databases, z.B.: template0,template1
#PMG_IGNORED_DATABASES=
# Zusätzliche Tags für alle Metriken (wie Telegraf global_tags), z.B.: env=test,dc=fra
#PMG_TAGS=
# Text-Feld datname wie Telegraf mitsenden (erzeugt nutzlose 0-Zeitreihen in VictoriaMetrics)
#PMG_STRING_FIELDS=1

# Beim Start N Minuten Historie erzeugen (0 = aus). Achtung: bei jedem Neustart!
PMG_BACKFILL=0
EOF
    chmod 0600 "${ENV_FILE}"
    umask 022
fi

log "Lege systemd-Service ${SERVICE_NAME} an ..."
cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=PostgreSQL Metrics Generator (Telegraf format) for VictoriaMetrics
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SVC_USER}
Group=${SVC_USER}
# Statistik-Zähler überleben Neustarts (/var/lib/${SERVICE_NAME}/state.json)
StateDirectory=${SERVICE_NAME}
Environment=PMG_STATE_FILE=/var/lib/${SERVICE_NAME}/state.json
EnvironmentFile=${ENV_FILE}
ExecStart=/usr/bin/python3 -u ${INSTALL_DIR}/${SCRIPT}
Restart=always
RestartSec=5

# Härtung
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
PrivateTmp=true
PrivateDevices=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictSUIDSGID=true
LockPersonality=true

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now "${SERVICE_NAME}.service"
sleep 3

if systemctl is-active --quiet "${SERVICE_NAME}"; then
    log "Fertig! Der Generator läuft und sendet an ${VM_URL_ARG}"
    journalctl -u "${SERVICE_NAME}" -n 3 --no-pager -o cat
    echo
    echo "    Status:        systemctl status ${SERVICE_NAME}"
    echo "    Logs:          journalctl -u ${SERVICE_NAME} -f"
    echo "    Einstellungen: ${ENV_FILE}"
else
    err "Service konnte nicht gestartet werden:"
    journalctl -u "${SERVICE_NAME}" -n 30 --no-pager
    exit 1
fi
