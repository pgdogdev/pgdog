#!/usr/bin/env bash
set -euo pipefail

killall toxiproxy-server 2>/dev/null && echo "Stopped toxiproxy-server." || echo "toxiproxy-server was not running."
