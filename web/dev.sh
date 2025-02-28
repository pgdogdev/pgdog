#!/bin/bash
#
# Launch a local dev "server".
#
# Use this for for "convenient" local dev.
#
set -e
source venv/bin/activate
echo "Launching dev \"server\""

python -m http.server &
pid=$!

trap "kill -TERM ${pid}; echo \"existing., bye\"; exit;" INT

open http://localhost:8000/ || true

while true; do
    python blog/blog.py > /dev/null
    sassc styles/style.scss > styles/style.css
    sleep 1
done
