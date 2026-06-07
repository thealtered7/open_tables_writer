#!/bin/sh
set -e

MAIN_CLASS="${1:-com.thealtered7.OpenTablesWriter}"
if [ "$#" -gt 0 ]; then
  shift
fi

exec java ${SPARK_JVM_ARGS:-} -cp "/app/lib/*" "$MAIN_CLASS" "$@"
