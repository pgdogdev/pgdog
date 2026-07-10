#!/bin/bash
touch log.txt
cargo run --features new_parser > log.txt 2>&1 &
pid=$!

trap shutdown INT

function shutdown() {
    kill -TERM $pid
}

tail -f log.txt
