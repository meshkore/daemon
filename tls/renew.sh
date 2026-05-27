#!/usr/bin/env bash
# Renew the loopback TLS bundle.
#
# Usage:   ./daemon/tls/renew.sh
# Run as:  cron, every 60 days, or manually before a daemon release.
#
# Reads the Cloudflare API token from
# .meshkore/credentials/cloudflare-token.txt (zone:read + dns:write
# on meshkore.com).
#
# On success:
#   - daemon/tls/fullchain.pem and daemon/tls/privkey.pem updated in place
#   - 0 exit code
#   - operator should commit + push so users pick up the new cert
#     on their next daemon upgrade
#
# On failure: prints what certbot said and exits non-zero.

set -euo pipefail

cd "$(git rev-parse --show-toplevel 2>/dev/null || dirname "$(dirname "$(realpath "$0")")")"

TOKEN_FILE=".meshkore/credentials/cloudflare-token.txt"
if [ ! -r "$TOKEN_FILE" ]; then
  echo "renew.sh: cannot read $TOKEN_FILE" >&2
  exit 1
fi

CERTBOT_BIN="${CERTBOT_BIN:-$(command -v certbot || echo /opt/homebrew/bin/certbot)}"
if [ ! -x "$CERTBOT_BIN" ]; then
  echo "renew.sh: certbot not found. Install with 'brew install certbot' (macOS)." >&2
  exit 1
fi

WORK_DIR=".meshkore/credentials/daemon-tls"
mkdir -p "$WORK_DIR"
CREDS_INI="$WORK_DIR/cloudflare-credentials.ini"
printf 'dns_cloudflare_api_token = %s\n' "$(cat "$TOKEN_FILE")" > "$CREDS_INI"
chmod 600 "$CREDS_INI"

"$CERTBOT_BIN" certonly \
  --dns-cloudflare \
  --dns-cloudflare-credentials "$CREDS_INI" \
  --dns-cloudflare-propagation-seconds 30 \
  -d 'daemon.meshkore.com' \
  -d '*.daemon.meshkore.com' \
  --agree-tos \
  --email ricart@charms.dev \
  --non-interactive \
  --config-dir "$WORK_DIR/certbot" \
  --work-dir "$WORK_DIR/certbot/work" \
  --logs-dir "$WORK_DIR/certbot/logs" \
  --key-type ecdsa \
  --preferred-chain "ISRG Root X1"

LIVE="$WORK_DIR/certbot/live/daemon.meshkore.com"
cp "$LIVE/fullchain.pem" daemon/tls/fullchain.pem
cp "$LIVE/privkey.pem"   daemon/tls/privkey.pem
chmod 644 daemon/tls/fullchain.pem
chmod 600 daemon/tls/privkey.pem

echo
echo "renew.sh: cert bundle refreshed."
openssl x509 -in daemon/tls/fullchain.pem -noout -subject -dates
echo
echo "Next: git add daemon/tls/{fullchain,privkey}.pem && git commit && git push"
