#!/usr/bin/env bash
#
# install-loggen.sh - Apache-JSON-Log-Generator als systemd-Service auf Debian 13
#
#  - legt den System-User "loggen" an
#  - kopiert apache_loggen.py nach /opt/apache-loggen
#  - legt /etc/default/apache-loggen mit den Einstellungen an
#  - richtet den systemd-Service "apache-loggen" ein und startet ihn
#
# Aufruf (im Verzeichnis mit apache_loggen.py):
#   sudo bash install-loggen.sh [VICTORIALOGS_URL]
#   z.B. sudo bash install-loggen.sh http://192.168.1.50:9428
#
set -euo pipefail

VL_URL="${1:-http://localhost:9428}"
SVC_USER="loggen"
INSTALL_DIR="/opt/apache-loggen"
ENV_FILE="/etc/default/apache-loggen"
SERVICE_NAME="apache-loggen"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
SRC_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

log() { echo -e "\e[1;32m[+]\e[0m $*"; }
err() { echo -e "\e[1;31m[!]\e[0m $*" >&2; }

[[ $EUID -eq 0 ]] || { err "Bitte als root ausführen (sudo bash $0)."; exit 1; }
[[ -f "${SRC_DIR}/apache_loggen.py" ]] || { err "apache_loggen.py nicht gefunden in ${SRC_DIR}."; exit 1; }

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
install -o root -g root -m 0755 "${SRC_DIR}/apache_loggen.py" "${INSTALL_DIR}/apache_loggen.py"

if [[ -f "${ENV_FILE}" ]]; then
    log "${ENV_FILE} existiert bereits - bleibt unverändert."
else
    log "Schreibe Einstellungen nach ${ENV_FILE} ..."
    cat > "${ENV_FILE}" <<EOF
# Einstellungen für apache-loggen (nach Änderung: systemctl restart ${SERVICE_NAME})

# Adresse von VictoriaLogs
VL_URL=${VL_URL}

# Logs pro Sekunde (zufällig zwischen MIN und MAX)
LOGGEN_MIN_RATE=2
LOGGEN_MAX_RATE=20

# Domain für die simulierten vhosts www., shop. und api.
LOGGEN_DOMAIN=example.de

# Felder, die den Log-Stream in VictoriaLogs bilden
LOGGEN_STREAM_FIELDS=vhost

# Beim Start N Minuten Historie erzeugen (0 = aus). Achtung: bei jedem Neustart!
LOGGEN_BACKFILL=0

# 1 = kein zusätzliches _msg-Feld
#LOGGEN_NO_MSG=1

# Multi-Tenancy (optional)
#VL_ACCOUNT_ID=0
#VL_PROJECT_ID=0
EOF
    chmod 0644 "${ENV_FILE}"
fi

log "Lege systemd-Service ${SERVICE_NAME} an ..."
cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=Apache JSON Log Generator for VictoriaLogs
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SVC_USER}
Group=${SVC_USER}
EnvironmentFile=${ENV_FILE}
ExecStart=/usr/bin/python3 -u ${INSTALL_DIR}/apache_loggen.py
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
    log "Fertig! Der Generator läuft und sendet an ${VL_URL}"
    echo "    Status:        systemctl status ${SERVICE_NAME}"
    echo "    Logs:          journalctl -u ${SERVICE_NAME} -f"
    echo "    Einstellungen: ${ENV_FILE}"
else
    err "Service konnte nicht gestartet werden:"
    journalctl -u "${SERVICE_NAME}" -n 30 --no-pager
    exit 1
fi
