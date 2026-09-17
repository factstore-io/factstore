#!/usr/bin/env bash
# Runs every limit sample against $BASE_URL (default: http://localhost:8080).
dir=$(dirname "$0")
for script in "$dir"/0*.sh; do
  bash "$script"
done
