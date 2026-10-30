#!/usr/bin/env bash
# Screenshot the Relay Grafana dashboard (local k3d cluster, anonymous viewer
# access enabled in deploy/k8s/grafana-values.yaml) with headless Chrome/Edge.
#   scripts/grafana-screenshots.sh [name] [minutes]
set -euo pipefail
NAME=${1:-dashboard}; MIN=${2:-10}
OUT="$(cd "$(dirname "$0")/.." && pwd)/docs/figures/grafana-$NAME.png"
BROWSER=${BROWSER:-$(command -v google-chrome || command -v chromium || echo "/c/Program Files/Google/Chrome/Application/chrome.exe")}
command -v cygpath >/dev/null && OUT=$(cygpath -w "$OUT")  # Windows browsers need a native path
URL="http://localhost:30300/d/relay-overview/relay?orgId=1&from=now-${MIN}m&to=now&kiosk"
"$BROWSER" --headless=new --disable-gpu --hide-scrollbars --window-size=1800,2700 \
  --virtual-time-budget=20000 --screenshot="$OUT" "$URL" >/dev/null 2>&1
echo "wrote $OUT"
