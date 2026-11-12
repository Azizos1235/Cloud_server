#!/bin/sh
# Render start script: Tailscale (userspace) + socat tunnels to the Pi, then Uvicorn.

SOCK=/tmp/tailscaled.sock
PI_HOST="${PI_TAILSCALE_HOST:-100.109.169.109}"   # Pi's Tailscale IP or MagicDNS name

ts_connect() {
  # tailscaled needs a moment to start, so retry the login instead of one blind sleep
  i=0
  while [ "$i" -lt 10 ]; do
    if tailscale --socket="$SOCK" up \
        --authkey="$TS_AUTHKEY" \
        --hostname=render-ingest \
        --accept-dns=false; then
      return 0
    fi
    i=$((i + 1))
    sleep 3
  done
  return 1
}

if [ -z "${TS_AUTHKEY:-}" ]; then
  echo "ERROR: TS_AUTHKEY is not set in the Render environment; skipping Tailscale and tunnels" >&2
else
  tailscaled --tun=userspace-networking --state=mem: --socket="$SOCK" &

  if ts_connect; then
    # local 127.0.0.1:5672 -> Pi's RabbitMQ through Tailscale
    socat TCP-LISTEN:5672,fork,reuseaddr,bind=127.0.0.1 \
      EXEC:"tailscale --socket=$SOCK nc $PI_HOST 5672" &

    # local 127.0.0.1:8001 -> Pi's Flask AI service through Tailscale
    # (Render env var: PI_AI_URL=http://127.0.0.1:8001)
    socat TCP-LISTEN:8001,fork,reuseaddr,bind=127.0.0.1 \
      EXEC:"tailscale --socket=$SOCK nc $PI_HOST 8001" &

    sleep 2
  else
    echo "ERROR: tailscale up failed after 10 tries (check TS_AUTHKEY: expired, single-use, or wrong?)" >&2
    echo "WARNING: starting the API without the Pi tunnels" >&2
  fi
fi

# Render's load balancer must reach the app, so bind to 0.0.0.0 on $PORT.
# (Binding to 127.0.0.1 makes Render's port check fail and the service unreachable.)
exec uvicorn app.main:app \
  --host 0.0.0.0 \
  --port "${PORT:-10000}" \
  --proxy-headers \
  --forwarded-allow-ips='*'