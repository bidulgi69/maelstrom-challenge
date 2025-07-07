#!/usr/bin/env sh
BASEDIR="$(cd "$(dirname "$0")" && pwd)"
exec java -jar "$BASEDIR/maelstrom-node/build/libs/maelstrom-node-1.0-SNAPSHOT.jar" "$@"