#!/bin/sh
SOCK=/tmp/tailscaled.sock
tailscaled --tun=userspace-networking --state=mem: --socket=$SOCK &
sleep 4
tailscale --socket=$SOCK up --authkey="$TS_AUTHKEY" --hostname=render-ingest
# local port 5672 -> Pi's broker through Tailscale
socat TCP-LISTEN:5672,fork,reuseaddr,bind=127.0.0.1 \
  EXEC:"tailscale --socket=$SOCK nc 100.109.169.109 5672" &
sleep 2
exec uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-10000}