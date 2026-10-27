#!/usr/bin/env bash
#
# install-loggen-opensearch.sh - Apache-JSON-Log-Generator für OpenSearch
#                                als systemd-Service auf Debian 13
#
#  - legt den System-User "loggen" an (falls noch nicht vorhanden)
#  - kopiert apache_loggen_opensearch.py nach /opt/apache-loggen
#  - legt /etc/default/apache-loggen-opensearch an (Rechte 0600, enthält das Passwort)
#  - richtet den systemd-Service "apache-loggen-opensearch" ein und startet ihn
#
# Aufruf (im Verzeichnis mit apache_loggen_opensearch.py):
#   sudo bash install-loggen-opensearch.sh [OPENSEARCH_URL] [BENUTZER]
#   z.B. sudo bash install-loggen-opensearch.sh https://192.168.1.60:9200 admin
#
# Das Passwort wird interaktiv abgefragt (oder per Umgebungsvariable OS_PASSWORD
# übergeben), damit es nicht in der Shell-History landet.
#
set -euo pipefail

OS_URL_ARG="${1:-https://localhost:9200}"
OS_USER_ARG="${2:-admin}"
SVC_USER="loggen"
INSTALL_DIR="/opt/apache-loggen"
ENV_FILE="/etc/default/apache-loggen-opensearch"
SERVICE_NAME="apache-loggen-opensearch"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
SCRIPT="apache_loggen_opensearch.py"
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
    if [[ -z "${OS_PASSWORD:-}" ]]; then
        [[ -t 0 ]] || { err "Kein Terminal: Passwort per OS_PASSWORD=... übergeben."; exit 1; }
        read -r -s -p "Passwort für OpenSearch-Benutzer '${OS_USER_ARG}': " OS_PASSWORD
        echo
    fi
    if [[ "${OS_PASSWORD}" == *"'"* ]]; then
        err "Das Passwort enthält ein einfaches Anführungszeichen (') - das kann die"
        err "systemd-Umgebungsdatei nicht sicher speichern. Bitte ${ENV_FILE} von Hand anlegen."
        exit 1
    fi
    log "Schreibe Einstellungen nach ${ENV_FILE} (nur für root lesbar) ..."
    umask 077
    cat > "${ENV_FILE}" <<EOF
# Einstellungen für ${SERVICE_NAME} (nach Änderung: systemctl restart ${SERVICE_NAME})

# --- OpenSearch -------------------------------------------------------------
OS_URL=${OS_URL_ARG}
OS_USER=${OS_USER_ARG}
OS_PASSWORD='${OS_PASSWORD}'

# TLS: CA-Zertifikat (PEM) zum Prüfen des Server-Zertifikats ...
#OS_CA_CERT=/etc/ssl/certs/opensearch-root-ca.pem
# ... oder Prüfung abschalten (nur für Tests/Demo-Zertifikate!)
#OS_INSECURE=1

# Indexname (strftime-Platzhalter, Datum in UTC) und Template
OS_INDEX=apache-logs-%Y.%m.%d
OS_TEMPLATE_NAME=apache-logs
# Replikate je Index: 0 für Single-Node, 1 für Cluster
OS_REPLICAS=0
OS_SHARDS=1

# --- Generator ----------------------------------------------------------------
LOGGEN_MIN_RATE=2
LOGGEN_MAX_RATE=20
LOGGEN_DOMAIN=example.de
# Beim Start N Minuten Historie erzeugen (0 = aus). Achtung: bei jedem Neustart!
LOGGEN_BACKFILL=0
#LOGGEN_NO_MSG=1
EOF
    chmod 0600 "${ENV_FILE}"
    umask 022
fi

log "Lege systemd-Service ${SERVICE_NAME} an ..."
cat > "${SERVICE_FILE}" <<EOF
[Unit]
Description=Apache JSON Log Generator for OpenSearch
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${SVC_USER}
Group=${SVC_USER}
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
sleep 4

if systemctl is-active --quiet "${SERVICE_NAME}"; then
    log "Service läuft. Letzte Meldungen:"
    journalctl -u "${SERVICE_NAME}" -n 5 --no-pager -o cat
    echo
    echo "    Status:        systemctl status ${SERVICE_NAME}"
    echo "    Logs:          journalctl -u ${SERVICE_NAME} -f"
    echo "    Einstellungen: ${ENV_FILE}"
    echo
    echo "    Hinweis: Bei selbstsignierten Zertifikaten (z.B. OpenSearch-Demo) in"
    echo "    ${ENV_FILE} OS_CA_CERT setzen oder OS_INSECURE=1 aktivieren."
else
    err "Service konnte nicht gestartet werden:"
    journalctl -u "${SERVICE_NAME}" -n 30 --no-pager
    exit 1
fi
