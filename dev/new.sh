#!/bin/bash
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

branch=$(echo "$USER-$@" | sed 's/\ /\-/g' | tr '[:upper]' '[:lower]')

git reset --hard origin/main
git checkout main
git pull origin main
git checkout -b "$branch"
