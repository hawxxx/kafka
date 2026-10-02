#!/bin/bash
#
# Spreads the topic among the active brokers.
# Does not change the replication factor.
#
# Partition reassignment for Amazon MSK 3.9.x KRaft clusters without ZooKeeper.
#
# Differences from the ZooKeeper-based script:
#   - --zookeeper <ZK_HOST>              -> --bootstrap-server <BOOT>:9094 --command-config <CLIENT_PROPERTIES>
#   - zookeeper-shell.sh ls /brokers/ids -> kafka-broker-api-versions.sh --bootstrap-server ...
#   - zookeepercli poll of /admin/reassign_partitions -> --verify exit status only (no znode under KRaft)
#   - kafka-preferred-replica-election.sh (deprecated) -> kafka-leader-election.sh --election-type PREFERRED
#
# Usage: $0 <BOOTSTRAP_SERVER> [<CLIENT_PROPERTIES>] <TOPICS_FILE_OR_LIST> [--dry-run | --rollback] [--throttle <THROTTLE_RATE>] [--brokers <BROKER_ID_LIST>]
#
# CLIENT_PROPERTIES is optional; defaults to /home/ubuntu/client.properties.
#
# TOPICS_FILE_OR_LIST accepts a path to a file (one topic per line) or a
# single/comma-separated topic name directly on the command line.
#
# --brokers <BROKER_ID_LIST> restricts --generate's broker pool to an explicit
# comma-separated set of broker IDs instead of auto-discovering the full cluster.

set -u

usage() {
  echo "Usage: $0 <BOOTSTRAP_SERVER> [<CLIENT_PROPERTIES>] <TOPICS_FILE_OR_LIST> [--dry-run | --rollback] [--throttle <THROTTLE_RATE>] [--brokers <BROKER_ID_LIST>]"
  echo
  echo "Arguments:"
  echo "  <BOOTSTRAP_SERVER>     MSK bootstrap broker(s), e.g. boot-example.kafka.us-east-1.amazonaws.com:9094"
  echo "  <CLIENT_PROPERTIES>    Optional. Path to client.properties (must set security.protocol=SSL)."
  echo "                         Defaults to /home/ubuntu/client.properties if omitted."
  echo "  <TOPICS_FILE_OR_LIST>  Either a path to a file listing topics (one per line), a single"
  echo "                         topic name, or a comma-separated list of topic names, e.g."
  echo "                         \"topicA,topicB\""
  echo
  echo "Options:"
  echo "  --dry-run                Generate the reassignment plan without executing it"
  echo "  --rollback                Rollback to the previous reassignment plan"
  echo "  --throttle <RATE>         Throttle rate in bytes per second for data transfer during reassignment"
  echo "  --brokers <BROKER_LIST>   Restrict --generate to this comma-separated set of broker IDs"
  echo "                            instead of auto-discovering every broker in the cluster."
  echo "  --no-progress             Disable the progress bar / ETA output (plain sequential logs only)"
  echo "  --help                    Display this help message"
  exit 1
}

if [[ "${1:-}" == "--help" ]]; then
  usage
fi

if [[ $# -lt 2 ]]; then
  usage
fi

# Override via KAFKA_HOME env var if different.
KAFKA_HOME="${KAFKA_HOME:-/home/ubuntu/kafka_2.13-2.8.2/}"
DEFAULT_CLIENT_CONFIG="/home/ubuntu/client.properties"

BOOT="$1"
MODE=""
THROTTLE=""
BROKERS_OVERRIDE=""
SHOW_PROGRESS=1

if [[ -f "${2:-}" && -n "${3:-}" && "${3:-}" != --* ]]; then
  # 3-positional-arg form: <BOOT> <CLIENT_PROPERTIES> <TOPICS_FILE>
  CLIENT_CONFIG="$2"
  TOPICS_FILE="$3"
  shift 3
else
  # 2-positional-arg form: <BOOT> <TOPICS_FILE>, client.properties auto-read from default path
  CLIENT_CONFIG="$DEFAULT_CLIENT_CONFIG"
  TOPICS_FILE="$2"
  shift 2
fi

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run|--rollback)
      MODE="$1"
      shift
      ;;
    --throttle)
      if [[ $# -lt 2 ]]; then
        echo "Error: --throttle requires a value"
        usage
      fi
      THROTTLE="$2"
      shift 2
      ;;
    --brokers)
      if [[ $# -lt 2 ]]; then
        echo "Error: --brokers requires a comma-separated list of broker IDs"
        usage
      fi
      BROKERS_OVERRIDE="$2"
      shift 2
      ;;
    --no-progress)
      SHOW_PROGRESS=0
      shift
      ;;
    *)
      echo "Invalid option: $1"
      usage
      ;;
  esac
done

if [[ -n "$MODE" && "$MODE" != "--dry-run" && "$MODE" != "--rollback" ]]; then
  echo "Invalid mode: $MODE"
  usage
fi

if [[ -n "$THROTTLE" && ! "$THROTTLE" =~ ^[0-9]+$ ]]; then
  echo "Invalid throttle rate: $THROTTLE"
  usage
fi

if [[ -n "$BROKERS_OVERRIDE" && ! "$BROKERS_OVERRIDE" =~ ^[0-9]+(,[0-9]+)*$ ]]; then
  echo "Invalid --brokers value: $BROKERS_OVERRIDE (expected comma-separated integers, e.g. 3,16,24)"
  usage
fi

[[ -n $BOOT ]] || {
  echo "Missing MSK bootstrap server"
  echo "Usage: $0 boot-example.kafka.us-east-1.amazonaws.com:9094 client.properties topics.txt [--dry-run | --rollback] [--throttle <THROTTLE_RATE>]"
  exit 2
}

[[ -f $CLIENT_CONFIG ]] || {
  echo "Missing client.properties file: $CLIENT_CONFIG"
  if [[ "$CLIENT_CONFIG" == "$DEFAULT_CLIENT_CONFIG" ]]; then
    echo "(using default path since none was passed explicitly)"
  fi
  echo "Minimum required content for these clusters (Unauthenticated / TLS-only):"
  echo "  security.protocol=SSL"
  exit 2
}
echo "Using client.properties: $CLIENT_CONFIG"

if [[ -f "$TOPICS_FILE" ]]; then
  echo "Using topics file: $TOPICS_FILE"
else
  if [[ -z "$TOPICS_FILE" ]]; then
    echo "Missing topics file or topic list"
    echo "Usage: $0 $BOOT $CLIENT_CONFIG topics.txt [--dry-run | --rollback] [--throttle <THROTTLE_RATE>]"
    exit 2
  fi
  echo "Treating '$TOPICS_FILE' as an inline topic name / comma-separated topic list (not an existing file path)."
  INLINE_TOPICS_FILE=$(mktemp --tmpdir "topics-inline.XXXXXXXX")
  IFS=',' read -ra INLINE_TOPICS <<< "$TOPICS_FILE"
  for T in "${INLINE_TOPICS[@]}"; do
    T="${T#"${T%%[![:space:]]*}"}"
    T="${T%"${T##*[![:space:]]}"}"
    [[ -n "$T" ]] && echo "$T" >> "$INLINE_TOPICS_FILE"
  done
  TOPICS_FILE="$INLINE_TOPICS_FILE"
  echo "Resolved inline topic(s) to temp file: $TOPICS_FILE"
  echo "Topics: $(paste -sd, "$TOPICS_FILE")"
fi

# ---------------------------------------------------------------------------
# Progress bar / ETA helpers
#
# TOTAL_TOPICS is computed once up front (excluding blank/comment lines, the
# same filter applied in the main processing loop below) so the counter and
# ETA are accurate regardless of file formatting.
#
# ETA is a simple running average: elapsed_time_so_far / topics_done, applied
# to the topics remaining. It naturally adapts as topic processing time varies
# (e.g. large vs small topics), rather than assuming a fixed per-topic cost.
# ---------------------------------------------------------------------------

TOTAL_TOPICS=$(grep -cv -e '^[[:space:]]*$' -e '^[[:space:]]*#' "$TOPICS_FILE" || true)
TOTAL_TOPICS=${TOTAL_TOPICS:-0}
TOPICS_DONE=0
RUN_START_EPOCH=$(date +%s)

format_duration() {
  # Formats a duration in seconds as Hh Mm Ss (omitting leading zero units).
  local total_seconds="$1"
  local h=$(( total_seconds / 3600 ))
  local m=$(( (total_seconds % 3600) / 60 ))
  local s=$(( total_seconds % 60 ))
  if   [[ $h -gt 0 ]]; then printf '%dh %dm %ds' "$h" "$m" "$s"
  elif [[ $m -gt 0 ]]; then printf '%dm %ds' "$m" "$s"
  else printf '%ds' "$s"
  fi
}

print_progress() {
  # print_progress <label>
  # Renders "[#####-----] N/Total (XX%) label | elapsed Xm Ys | ETA ~Xm Ys"
  [[ "$SHOW_PROGRESS" -eq 1 ]] || return 0
  local label="${1:-}"
  local total="$TOTAL_TOPICS"
  [[ "$total" -gt 0 ]] || { echo "$label"; return 0; }

  local done="$TOPICS_DONE"
  local now percent filled empty bar elapsed eta_str elapsed_str

  now=$(date +%s)
  elapsed=$(( now - RUN_START_EPOCH ))
  percent=$(( done * 100 / total ))

  local bar_width=30
  filled=$(( bar_width * done / total ))
  [[ $filled -gt $bar_width ]] && filled=$bar_width
  empty=$(( bar_width - filled ))

  bar=""
  [[ $filled -gt 0 ]] && bar+=$(printf '%*s' "$filled" '' | tr ' ' '#')
  [[ $empty -gt 0 ]] && bar+=$(printf '%*s' "$empty" '' | tr ' ' '-')

  elapsed_str=$(format_duration "$elapsed")

  if [[ "$done" -gt 0 ]]; then
    local avg_per_topic remaining_topics eta_seconds
    avg_per_topic=$(( elapsed / done ))
    remaining_topics=$(( total - done ))
    eta_seconds=$(( avg_per_topic * remaining_topics ))
    eta_str="~$(format_duration "$eta_seconds")"
  else
    eta_str="calculating..."
  fi

  printf '[%s] %d/%d (%d%%) | elapsed %s | ETA %s | %s\n' \
    "$bar" "$done" "$total" "$percent" "$elapsed_str" "$eta_str" "$label"
}

BACKUP_DIR="$PWD/backup_reassignment_plan"
REPORT_FILE="$PWD/reassignment_report.txt"
mkdir -p "$BACKUP_DIR"
chmod 755 "$BACKUP_DIR"

echo -e "+---------------------------------------------------------+-----------------+-----------------+-----------------+" > "$REPORT_FILE"
echo -e "| Topic                                                   | Partition       | Previous Brokers| New Brokers     |" >> "$REPORT_FILE"
echo -e "+---------------------------------------------------------+-----------------+-----------------+-----------------+" >> "$REPORT_FILE"

if [[ -n "$BROKERS_OVERRIDE" ]]; then
  BROKERS="$BROKERS_OVERRIDE"
  echo "Using explicit --brokers override (skipping full-cluster discovery): $BROKERS"
else
  BROKERS="$("$KAFKA_HOME/bin/kafka-broker-api-versions.sh" \
      --bootstrap-server "$BOOT" \
      --command-config "$CLIENT_CONFIG" 2>/dev/null \
      | grep -oP '\(id: \K[0-9]+' \
      | sort -un \
      | paste -sd, -)"

  if [[ -z "$BROKERS" ]]; then
    echo "Error: could not discover any broker IDs via kafka-broker-api-versions.sh. Check bootstrap server and client.properties."
    exit 3
  fi
  echo "Brokers: $BROKERS"
fi

echo "Total topics to process: $TOTAL_TOPICS"

trap 'echo "Script interrupted. Exiting..."; exit 1' SIGINT SIGTERM

while IFS= read -r TOPIC; do
  [[ -z "$TOPIC" || "$TOPIC" =~ ^# ]] && continue

  print_progress "Starting: $TOPIC"
  echo -e "\n▂▃▅▇█▓▒░ Processing topic: $TOPIC ░▒▓█▇▅▃▂\n"
  echo "Processing topic: $TOPIC"

  WORKDIR=$(mktemp --directory --tmpdir "${TOPIC}.XXXXXXXX")
  cd "$WORKDIR"

  TOPICS_JSON="${TOPIC}.topics.json"
  JSON_OUT="${WORKDIR}/${TOPIC}.reassign.json"
  BACKUP_JSON="${BACKUP_DIR}/${TOPIC}.backup.json"

  echo -n "{\"topics\":[{\"topic\": \"$TOPIC\"}], \"version\":1}" > "$TOPICS_JSON"

  if [[ "$MODE" == "--rollback" ]]; then
    if [[ -f "$BACKUP_JSON" ]]; then
      echo "Rolling back topic: $TOPIC using backup plan: $BACKUP_JSON"
      "$KAFKA_HOME/bin/kafka-reassign-partitions.sh" \
          --bootstrap-server "$BOOT" \
          --command-config "$CLIENT_CONFIG" \
          --reassignment-json-file "$BACKUP_JSON" \
          --execute

      while ! "$KAFKA_HOME/bin/kafka-reassign-partitions.sh" \
          --bootstrap-server "$BOOT" \
          --command-config "$CLIENT_CONFIG" \
          --reassignment-json-file "$BACKUP_JSON" \
          --verify > /dev/null; do
          echo 'Rollback in progress...'
          sleep 5s
      done

      echo 'Rollback complete.'

      # Mirror the forward path so leadership also returns to the backup's original
      # leader, not whatever broker became leader during the forward reassignment.
      ROLLBACK_LEADER_ELECTION_JSON="${WORKDIR}/${TOPIC}.rollback-leader-election.json"
      jq -c --arg TOPIC "$TOPIC" \
          '{"partitions": [.partitions[] | {"topic": $TOPIC, "partition": .partition}]}' \
          "$BACKUP_JSON" > "$ROLLBACK_LEADER_ELECTION_JSON"

      echo "Triggering preferred leader election for $TOPIC after rollback..."
      "$KAFKA_HOME/bin/kafka-leader-election.sh" \
          --bootstrap-server "$BOOT" \
          --admin.config "$CLIENT_CONFIG" \
          --election-type PREFERRED \
          --path-to-json-file "$ROLLBACK_LEADER_ELECTION_JSON"
      echo 'Done.'

      CURRENT_ASSIGNMENT=$(cat "$BACKUP_JSON")
      PROPOSED_ASSIGNMENT=$(cat "$BACKUP_JSON")

      PARTITIONS=$(echo "$CURRENT_ASSIGNMENT" | jq -r '.partitions[].partition' | sort -u)
      for PARTITION in $PARTITIONS; do
        PREVIOUS_BROKERS=$(echo "$CURRENT_ASSIGNMENT" | jq -r --arg PARTITION "$PARTITION" '.partitions[] | select(.partition == ($PARTITION | tonumber)) | .replicas | map(tostring) | join(",")')
        NEW_BROKERS=$(echo "$PROPOSED_ASSIGNMENT" | jq -r --arg PARTITION "$PARTITION" '.partitions[] | select(.partition == ($PARTITION | tonumber)) | .replicas | map(tostring) | join(",")')

        echo "Topic: $TOPIC, Partition: $PARTITION, Previous Brokers: [$PREVIOUS_BROKERS], New Brokers: [$NEW_BROKERS]"

        if [[ -z "$PREVIOUS_BROKERS" || -z "$NEW_BROKERS" ]]; then
          echo "Error: Failed to extract brokers from JSON files for partition $PARTITION. Skipping partition."
          continue
        fi

        printf "| %-55s | %-15s | %-15s | %-15s |\n" "$TOPIC" "$PARTITION" "$PREVIOUS_BROKERS" "$NEW_BROKERS" >> "$REPORT_FILE"
      done
    else
      echo "Backup plan for topic $TOPIC not found. Skipping rollback."
    fi
    cd - > /dev/null
    TOPICS_DONE=$(( TOPICS_DONE + 1 ))
    print_progress "Finished: $TOPIC"
    continue
  fi

  echo "Gathering the current assignment for backup..."
  BACKUP_OUTPUT=$("$KAFKA_HOME/bin/kafka-reassign-partitions.sh" \
      --bootstrap-server "$BOOT" \
      --command-config "$CLIENT_CONFIG" \
      --topics-to-move-json-file "$TOPICS_JSON" \
      --broker-list "$BROKERS" \
      --generate 2>&1)

  echo "$BACKUP_OUTPUT" | awk '/Current partition replica assignment/{flag=1; next} /Proposed partition reassignment configuration/{flag=0} flag' | awk 'NF' > "$BACKUP_JSON"

  if [[ ! -s "$BACKUP_JSON" ]]; then
    echo "Error: Backup JSON file not created: $BACKUP_JSON"
    echo "Command output: $BACKUP_OUTPUT"
    cd - > /dev/null
    TOPICS_DONE=$(( TOPICS_DONE + 1 ))
    print_progress "Failed: $TOPIC"
    continue
  fi

  echo "Backup JSON file created: $BACKUP_JSON"
  echo "Content of backup JSON file ($BACKUP_JSON):"
  cat "$BACKUP_JSON"

  echo "Gathering the reassignment plan..."
  REASSIGN_OUTPUT=$("$KAFKA_HOME/bin/kafka-reassign-partitions.sh" \
      --bootstrap-server "$BOOT" \
      --command-config "$CLIENT_CONFIG" \
      --broker-list "$BROKERS" \
      --topics-to-move-json-file "$TOPICS_JSON" \
      --generate 2>&1)
  echo "$REASSIGN_OUTPUT" | tail -n 1 > "$JSON_OUT"

  if [[ ! -s "$JSON_OUT" ]]; then
    echo "Error: Reassignment JSON file not created: $JSON_OUT"
    echo "Command output: $REASSIGN_OUTPUT"
    cd - > /dev/null
    TOPICS_DONE=$(( TOPICS_DONE + 1 ))
    print_progress "Failed: $TOPIC"
    continue
  fi

  echo "Content of reassignment JSON file ($JSON_OUT):"
  cat "$JSON_OUT"

  CURRENT_ASSIGNMENT=$(echo "$REASSIGN_OUTPUT" | sed -n '/Current partition replica assignment/,/Proposed partition reassignment configuration/p' | grep -o '{.*}')
  PROPOSED_ASSIGNMENT=$(echo "$REASSIGN_OUTPUT" | sed -n '/Proposed partition reassignment configuration/,$p' | grep -o '{.*}')

  PARTITIONS=$(echo "$CURRENT_ASSIGNMENT" | jq -r '.partitions[].partition' | sort -u)
  for PARTITION in $PARTITIONS; do
    PREVIOUS_BROKERS=$(echo "$CURRENT_ASSIGNMENT" | jq -r --arg PARTITION "$PARTITION" '.partitions[] | select(.partition == ($PARTITION | tonumber)) | .replicas | map(tostring) | join(",")')
    NEW_BROKERS=$(echo "$PROPOSED_ASSIGNMENT" | jq -r --arg PARTITION "$PARTITION" '.partitions[] | select(.partition == ($PARTITION | tonumber)) | .replicas | map(tostring) | join(",")')

    echo "Topic: $TOPIC, Partition: $PARTITION, Previous Brokers: [$PREVIOUS_BROKERS], New Brokers: [$NEW_BROKERS]"

    if [[ -z "$PREVIOUS_BROKERS" || -z "$NEW_BROKERS" ]]; then
      echo "Error: Failed to extract brokers from JSON files for partition $PARTITION. Skipping partition."
      continue
    fi

    printf "| %-55s | %-15s | %-15s | %-15s |\n" "$TOPIC" "$PARTITION" "$PREVIOUS_BROKERS" "$NEW_BROKERS" >> "$REPORT_FILE"
  done

  if [[ "$MODE" == "--dry-run" ]]; then
    echo "Dry run mode: Report updated for topic $TOPIC."
    echo "Reassignment JSON file path: $JSON_OUT"
  else
    echo "Starting the reassignment..."
    if [[ -n "$THROTTLE" ]]; then
      "$KAFKA_HOME/bin/kafka-reassign-partitions.sh" \
          --bootstrap-server "$BOOT" \
          --command-config "$CLIENT_CONFIG" \
          --reassignment-json-file "$JSON_OUT" \
          --execute --throttle "$THROTTLE"
    else
      "$KAFKA_HOME/bin/kafka-reassign-partitions.sh" \
          --bootstrap-server "$BOOT" \
          --command-config "$CLIENT_CONFIG" \
          --reassignment-json-file "$JSON_OUT" \
          --execute
    fi

    while ! OUTPUT=$("$KAFKA_HOME/bin/kafka-reassign-partitions.sh" \
        --bootstrap-server "$BOOT" \
        --command-config "$CLIENT_CONFIG" \
        --reassignment-json-file "$JSON_OUT" \
        --verify); do
        echo "$OUTPUT"
        sleep 5s
    done

    echo 'Reassignment complete.'

    # kafka-leader-election.sh requires --admin.config (not --command-config) on this
    # client build, and --topic/--all-topic-partitions are mutually exclusive, so scope
    # via --path-to-json-file with just this topic's partitions.
    LEADER_ELECTION_JSON="${WORKDIR}/${TOPIC}.leader-election.json"
    jq -c --arg TOPIC "$TOPIC" \
        '{"partitions": [.partitions[] | {"topic": $TOPIC, "partition": .partition}]}' \
        "$JSON_OUT" > "$LEADER_ELECTION_JSON"

    echo "Triggering preferred leader election for $TOPIC..."
    "$KAFKA_HOME/bin/kafka-leader-election.sh" \
        --bootstrap-server "$BOOT" \
        --admin.config "$CLIENT_CONFIG" \
        --election-type PREFERRED \
        --path-to-json-file "$LEADER_ELECTION_JSON"
    echo 'Done.'

    echo "Reassignment and preferred leader election confirmed complete for $TOPIC (verified via --verify exit status; no ZK node to poll under KRaft)."
  fi

  cd - > /dev/null
  TOPICS_DONE=$(( TOPICS_DONE + 1 ))
  print_progress "Finished: $TOPIC"
done < "$TOPICS_FILE"

echo -e "+---------------------------------------------------------+-----------------+-----------------+-----------------+" >> "$REPORT_FILE"

TOTAL_ELAPSED=$(( $(date +%s) - RUN_START_EPOCH ))
echo "All topics processed: $TOPICS_DONE/$TOTAL_TOPICS in $(format_duration "$TOTAL_ELAPSED")"
echo "Final Reassignment Report:"
cat "$REPORT_FILE"
