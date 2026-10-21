#!/usr/bin/env sh
# Builds the React console, runs Java + console tests, and packages target/conductor.jar.
set -eu
cd "$(dirname "$0")/.."
./mvnw -B package "$@"
