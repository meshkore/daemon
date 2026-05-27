# daemon/tls — bundled loopback TLS

This directory ships with a Let's Encrypt wildcard certificate for
`*.daemon.meshkore.com`. `daemon.meshkore.com` resolves to
`127.0.0.1` (public Cloudflare DNS A record), so when the daemon
serves TLS with this cert, any HTTPS origin (the cockpit at
`architect.meshkore.com`, your own dashboards, etc.) can talk to
the local daemon without:

- mixed-content rejections (HTTPS page → `ws://localhost` was blocked)
- Chrome Local Network Access "Issues" (every public→loopback fetch was flagged)

**The certificate and private key are deliberately public.** The
only thing an attacker who downloads them can do is impersonate
`daemon.meshkore.com` on their own loopback, which grants access
to nothing. Same pattern as Plex's `*.plex.direct`, Caddy 2's
local TLS, mkcert-issued wildcards, etc.

## How it works

1. `daemon.py` looks for `tls/fullchain.pem` + `tls/privkey.pem`
   relative to itself. When both exist and are readable, it builds
   an `ssl.SSLContext`, wraps the listening socket, and serves
   HTTPS + WSS on its port range (5570-5589).
2. `/health` advertises `tls: true` and exposes an `endpoint` field
   so the cockpit can switch URLs deterministically:
   - `tls: true`  → `https://daemon.meshkore.com:<port>`
   - `tls: false` → `http://localhost:<port>` (legacy)
3. If either file is missing or unreadable, the daemon logs the
   reason and falls back to plain HTTP. **Existing operators who
   don't pull the `tls/` directory keep working exactly as before.**

## Cert metadata

```
issuer:   Let's Encrypt (ISRG Root X1 chain)
subject:  daemon.meshkore.com
SAN:      daemon.meshkore.com, *.daemon.meshkore.com
key:      ECDSA P-256
expires:  ~ 90 days from issue (renewed by `renew.sh`)
```

Check the live values with:

```bash
openssl x509 -in daemon/tls/fullchain.pem -noout -subject -issuer -dates -ext subjectAltName
```

## Rotation

Let's Encrypt certs expire every 90 days. The `renew.sh` script
re-runs `certbot` with the Cloudflare DNS-01 plugin, copies the
fresh files in place, and prints a summary. Run it from the repo
root or via cron / a CI job every ~60 days:

```bash
./daemon/tls/renew.sh
```

Prerequisites for renewal:
- `certbot` installed (`brew install certbot` on macOS).
- The `certbot-dns-cloudflare` plugin (comes bundled with the brew
  install on macOS; Linux distros: `pip install certbot-dns-cloudflare`).
- A Cloudflare API token with `Zone DNS Edit` on `meshkore.com`.
  Lives at `.meshkore/credentials/cloudflare-token.txt`.

After a successful renewal, commit the updated `fullchain.pem` and
`privkey.pem` to the daemon repo. Operators pick up the new cert
on the next daemon update via the standard upgrade flow.

## What to do if `daemon.meshkore.com` ever changes

If we ever move the loopback subdomain (e.g. rebrand to
`local.meshkore.com`), three things must change in lock-step:

1. New CF DNS A record for the new name → 127.0.0.1.
2. Reissue cert with `renew.sh` pointing at the new SAN.
3. Update `LOOPBACK_HOSTNAME` in `architect/src/lib/transport.ts`
   and bump `DAEMON_VERSION` in `daemon.py` (cockpit MIN gate
   forces the upgrade).
