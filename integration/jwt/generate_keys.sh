#!/usr/bin/env bash
# Generate RSA key pair for JWT signing in local tests.
set -euo pipefail

KEYS_DIR="$(cd "$(dirname "$0")" && pwd)/keys"
mkdir -p "$KEYS_DIR"

if [[ -f "$KEYS_DIR/private.pem" && -f "$KEYS_DIR/public.pem" ]]; then
    echo "Keys already exist in $KEYS_DIR — skipping generation."
    exit 0
fi

openssl genrsa -out "$KEYS_DIR/private.pem" 2048
openssl rsa -in "$KEYS_DIR/private.pem" -pubout -out "$KEYS_DIR/public.pem"

chmod 600 "$KEYS_DIR/private.pem"
chmod 644 "$KEYS_DIR/public.pem"

echo "Keys written to $KEYS_DIR"
