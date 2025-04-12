#!/bin/bash
docker run \
    -e GEL_SERVER_SECURITY=insecure_dev_mode \
    -e GEL_SERVER_BACKEND_DSN=postgres://pgdog:pgdog@127.0.0.1:6432/pgdog \
    --network=host \
    geldata/gel:latest
