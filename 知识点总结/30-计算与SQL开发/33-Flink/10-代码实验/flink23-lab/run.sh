#!/usr/bin/env bash
set -euo pipefail
LAB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODE="${1:-windows}"
case "$MODE" in
  wordcount|windows|dedup|timers|topn|sql) MAIN_CLASS=notes.flink.Flink23Lab ;;
  plans) MAIN_CLASS=notes.flink.SqlPlanChecks ;;
  *) echo 'Usage: bash run.sh {wordcount|windows|dedup|timers|topn|sql|plans}' >&2; exit 2 ;;
esac
mvn -B -f "$LAB_DIR/pom.xml" compile dependency:build-classpath \
  -Dmdep.outputFile="$LAB_DIR/target/classpath.txt"
TASK_CP="$(cat "$LAB_DIR/target/classpath.txt")"
exec java \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
  -cp "$LAB_DIR/target/classes:$TASK_CP" "$MAIN_CLASS" "$MODE"
