#!/bin/bash
docker run \
    -e GEL_SERVER_PASSWORD=gel \
    -e GEL_SERVER_TLS_CERT_MODE=generate_self_signed \
    -e GEL_SERVER_BACKEND_DSN=postgres://pgdog:pgdog@127.0.0.1:6432/pgdog \
    --network=host \
    geldata/gel:latest
