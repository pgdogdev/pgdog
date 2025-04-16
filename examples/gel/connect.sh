#!/bin/bash

echo "pgdog" | gel --tls-ca-file cert.pem -P 5656 -u pgdog instance link my_instance --host 127.0.0.1 --branch default --password-from-stdin
