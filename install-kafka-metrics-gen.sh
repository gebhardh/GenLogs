#!/usr/bin/env bash
#
# install-kafka-metrics-gen.sh - Kafka-Metrik-Generator (Telegraf-Format) als
#                                systemd-Service auf Debian/Ubuntu
#
#  - legt den System-User "metricsgen" an
#  - kopiert kafka_metrics_gen.py nach /opt/kafka-metrics-gen
#  - legt /etc/default/kafka-metrics-gen mit den Einstellungen an
#  - richtet den systemd-Service "kafka-metrics-gen" ein und startet ihn
#
# Aufruf (im Verzeichnis mit kafka_metrics_gen.py):
#   sudo bash install-kafka-metrics-gen.sh [VICTORIAMETRICS_URL]
#   z.B. sudo bash install-kafka-metrics-gen.sh http://192.168.1.50:8428
#
set -euo pipefail

VM_URL_ARG="${1:-http://localhost:8428}"
SVC_USER="metricsgen"
INSTALL_DIR="/opt/kafka-metrics-gen"
SCRIPT="kafka_metrics_gen.py"
ENV_FILE="/etc/default/kafka-metrics-gen"
SERVICE_NAME="kafka-metrics-gen"
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

# --- Simulierter Cluster -----------------------------------------------------
# Erfassungsintervall in Sekunden (wie Telegraf "interval")
KMG_INTERVAL=10
KMG_BROKERS=3
KMG_HOST_PREFIX=kafka-
KMG_CLUSTER=kafka-prod
# Eigene Topics statt der Beispiel-Topics, z.B.: orders:12,payments:6
#KMG_TOPICS=
# Zusätzliche Tags für alle Metriken (wie Telegraf global_tags), z.B.: env=test,dc=fra
#KMG_TAGS=
#KMG_HEAP_GB=6

# ZooKeeper-Modus (Kafka < 4) mit kafka_zookeeper-Metriken
#KMG_ZOOKEEPER=1
# Text-Felder wie Telegraf mitsenden (erzeugt nutzlose 0-Zeitreihen in VictoriaMetrics)
#KMG_STRING_FIELDS=1
# Teile abschalten
#KMG_NO_BURROW=1
#KMG_NO_JAVA=1

# Beim Start N Minuten Historie erzeugen (0 = aus). Achtung: bei jedem Neustart!
KMG_BACKFILL=0
EOF
    chmod 0600 "${ENV_FILE}"
    umask 022
fi

log "Lege systemd-Service ${SERVICE_NAME} an ..."
cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=Kafka Metrics Generator (Telegraf format) for VictoriaMetrics
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SVC_USER}
Group=${SVC_USER}
# Topic- und Consumer-Offsets überleben Neustarts (/var/lib/${SERVICE_NAME}/state.json)
StateDirectory=${SERVICE_NAME}
Environment=KMG_STATE_FILE=/var/lib/${SERVICE_NAME}/state.json
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
