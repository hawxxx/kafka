#!/bin/bash
#
# Spreads the topic among the active brokers.
# Does not change the replication factor.
#
# Usage: $0 <ZOOKEEPER> <TOPICS_FILE> [--dry-run | --rollback] [--throttle <THROTTLE_RATE>]

set -u

# Function to display usage information
usage() {
  echo "Usage: $0 <ZOOKEEPER> <TOPICS_FILE> [--dry-run | --rollback] [--throttle <THROTTLE_RATE>]"
  echo
  echo "Arguments:"
  echo "  <ZOOKEEPER>          Kafka Zookeeper hostname or IP address"
  echo "  <TOPICS_FILE>        File containing the list of topics to rebalance"
  echo
  echo "Options:"
  echo "  --dry-run            Generate the reassignment plan without executing it"
  echo "  --rollback           Rollback to the previous reassignment plan"
  echo "  --throttle <RATE>    Throttle rate in bytes per second for data transfer during reassignment"
  echo "  --help               Display this help message"
  exit 1
}

# Check for --help argument
if [[ "$1" == "--help" ]]; then
  usage
fi

if [[ $# -lt 2 ]]; then
  usage
fi

KAFKA_HOME="/usr/local/kafka/"
ZOOKEEPER="$1"
TOPICS_FILE="$2"
MODE=""
THROTTLE=""

# Parse command-line arguments
shift 2
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
    *)
      echo "Invalid option: $1"
      usage
      ;;
  esac
done

# Validate MODE argument
if [[ -n "$MODE" && "$MODE" != "--dry-run" && "$MODE" != "--rollback" ]]; then
  echo "Invalid mode: $MODE"
  usage
fi

# Validate THROTTLE argument
if [[ -n "$THROTTLE" && ! "$THROTTLE" =~ ^[0-9]+$ ]]; then
  echo "Invalid throttle rate: $THROTTLE"
  usage
fi

[[ -n $ZOOKEEPER ]] || {
  echo "Missing Kafka zookeeper hostname or IP address"
  echo "Usage: $0 zookeeper.example.com topics.txt [--dry-run | --rollback] [--throttle <THROTTLE_RATE>]"
  exit 2
}

[[ -f $TOPICS_FILE ]] || {
  echo "Missing topics file"
  echo "Usage: $0 $ZOOKEEPER topics.txt [--dry-run | --rollback] [--throttle <THROTTLE_RATE>]"
  exit 2
}

# Define the backup directory in the current working directory
BACKUP_DIR="$PWD/backup_reassignment_plan"
REPORT_FILE="$PWD/reassignment_report.txt"
mkdir -p "$BACKUP_DIR"  # Ensure the backup directory is created
chmod 755 "$BACKUP_DIR" # Set proper permissions

echo -e "+---------------------------------------------------------+-----------------+-----------------+-----------------+" > "$REPORT_FILE"
echo -e "| Topic                                                   | Partition       | Previous Brokers| New Brokers     |" >> "$REPORT_FILE"
echo -e "+---------------------------------------------------------+-----------------+-----------------+-----------------+" >> "$REPORT_FILE"

BROKERS="$($KAFKA_HOME/bin/zookeeper-shell.sh "$ZOOKEEPER" ls /brokers/ids | tail -n 1 | sed 's/[^,[:digit:]]//g')"
echo "Brokers: $BROKERS"

# Signal trapping for graceful exit
trap 'echo "Script interrupted. Exiting..."; exit 1' SIGINT SIGTERM

while IFS= read -r TOPIC; do
  echo -e "\n▂▃▅▇█▓▒░ Processing topic: $TOPIC ░▒▓█▇▅▃▂\n"
  echo "Processing topic: $TOPIC"

  WORKDIR=$(mktemp --directory --tmpdir "${TOPIC}.XXXXXXXX")
  cd "$WORKDIR"

  TOPICS_JSON="${TOPIC}.topics.json"
  JSON_OUT="${WORKDIR}/${TOPIC}.reassign.json"
  BACKUP_JSON="${BACKUP_DIR}/${TOPIC}.backup.json" # Use the backup directory in the current working directory

  echo -n "{\"topics\":[{\"topic\": \"$TOPIC\"}], \"version\":1}" > "$TOPICS_JSON"

  # If rollback mode is enabled
  if [[ "$MODE" == "--rollback" ]]; then
    if [[ -f "$BACKUP_JSON" ]]; then
      echo "Rolling back topic: $TOPIC using backup plan: $BACKUP_JSON"
      "$KAFKA_HOME/bin/kafka-reassign-partitions.sh" \
          --zookeeper "$ZOOKEEPER" \
          --reassignment-json-file "$BACKUP_JSON" \
          --execute

      while ! "$KAFKA_HOME/bin/kafka-reassign-partitions.sh" \
          --zookeeper "$ZOOKEEPER" \
          --reassignment-json-file "$BACKUP_JSON" \
          --verify > /dev/null; do
          echo 'Rollback in progress...'
          sleep 5s
      done

      echo 'Rollback complete.'

      # Extract the current and proposed partition assignments
      CURRENT_ASSIGNMENT=$(cat "$BACKUP_JSON")
      PROPOSED_ASSIGNMENT=$(cat "$BACKUP_JSON")

      # Extract brokers for each partition
      PARTITIONS=$(echo "$CURRENT_ASSIGNMENT" | jq -r '.partitions[].partition' | sort -u)
      for PARTITION in $PARTITIONS; do
        PREVIOUS_BROKERS=$(echo "$CURRENT_ASSIGNMENT" | jq -r --arg PARTITION "$PARTITION" '.partitions[] | select(.partition == ($PARTITION | tonumber)) | .replicas | map(tostring) | join(",")')
        NEW_BROKERS=$(echo "$PROPOSED_ASSIGNMENT" | jq -r --arg PARTITION "$PARTITION" '.partitions[] | select(.partition == ($PARTITION | tonumber)) | .replicas | map(tostring) | join(",")')

        # Print extracted brokers
        echo "Topic: $TOPIC, Partition: $PARTITION, Previous Brokers: [$PREVIOUS_BROKERS], New Brokers: [$NEW_BROKERS]"

        # Check if brokers were extracted successfully
        if [[ -z "$PREVIOUS_BROKERS" || -z "$NEW_BROKERS" ]]; then
          echo "Error: Failed to extract brokers from JSON files for partition $PARTITION. Skipping partition."
          continue
        fi

        # Append to report file
        printf "| %-55s | %-15s | %-15s | %-15s |\n" "$TOPIC" "$PARTITION" "$PREVIOUS_BROKERS" "$NEW_BROKERS" >> "$REPORT_FILE"
      done
    else
      echo "Backup plan for topic $TOPIC not found. Skipping rollback."
    fi
    cd - > /dev/null  # Suppress the output of 'cd -'
    continue
  fi

  echo "Gathering the current assignment for backup..."
  BACKUP_OUTPUT=$("$KAFKA_HOME/bin/kafka-reassign-partitions.sh" \
      --zookeeper "$ZOOKEEPER" \
      --topics-to-move-json-file "$TOPICS_JSON" \
      --broker-list "$BROKERS" \
      --generate 2>&1)

  # Extract the line after "Current partition replica assignment" and before "Proposed partition reassignment configuration"
  echo "$BACKUP_OUTPUT" | awk '/Current partition replica assignment/{flag=1; next} /Proposed partition reassignment configuration/{flag=0} flag' | awk 'NF' > "$BACKUP_JSON"

  # Check if the backup JSON file was created
  if [[ ! -s "$BACKUP_JSON" ]]; then
    echo "Error: Backup JSON file not created: $BACKUP_JSON"
    echo "Command output: $BACKUP_OUTPUT"
    cd - > /dev/null  # Suppress the output of 'cd -'
    continue
  fi

  echo "Backup JSON file created: $BACKUP_JSON"
  # Show content of the Backup JSON
  echo "Content of backup JSON file ($BACKUP_JSON):"
  cat "$BACKUP_JSON"

  echo "Gathering the reassignment plan..."
  REASSIGN_OUTPUT=$("$KAFKA_HOME/bin/kafka-reassign-partitions.sh" \
      --zookeeper "$ZOOKEEPER" \
      --broker-list "$BROKERS" \
      --topics-to-move-json-file "$TOPICS_JSON" \
      --generate 2>&1)
  echo "$REASSIGN_OUTPUT" | tail -n 1 > "$JSON_OUT"

  # Check if the reassignment JSON file was created
  if [[ ! -s "$JSON_OUT" ]]; then
    echo "Error: Reassignment JSON file not created: $JSON_OUT"
    echo "Command output: $REASSIGN_OUTPUT"
    cd - > /dev/null  # Suppress the output of 'cd -'
    continue
  fi

  # Show content of the reassignment JSON
  echo "Content of reassignment JSON file ($JSON_OUT):"
  cat "$JSON_OUT"

  # Extract the current and proposed partition assignments
  CURRENT_ASSIGNMENT=$(echo "$REASSIGN_OUTPUT" | sed -n '/Current partition replica assignment/,/Proposed partition reassignment configuration/p' | grep -o '{.*}')
  PROPOSED_ASSIGNMENT=$(echo "$REASSIGN_OUTPUT" | sed -n '/Proposed partition reassignment configuration/,$p' | grep -o '{.*}')

  # Extract brokers for each partition
  PARTITIONS=$(echo "$CURRENT_ASSIGNMENT" | jq -r '.partitions[].partition' | sort -u)
  for PARTITION in $PARTITIONS; do
    PREVIOUS_BROKERS=$(echo "$CURRENT_ASSIGNMENT" | jq -r --arg PARTITION "$PARTITION" '.partitions[] | select(.partition == ($PARTITION | tonumber)) | .replicas | map(tostring) | join(",")')
    NEW_BROKERS=$(echo "$PROPOSED_ASSIGNMENT" | jq -r --arg PARTITION "$PARTITION" '.partitions[] | select(.partition == ($PARTITION | tonumber)) | .replicas | map(tostring) | join(",")')

    # Print extracted brokers
    echo "Topic: $TOPIC, Partition: $PARTITION, Previous Brokers: [$PREVIOUS_BROKERS], New Brokers: [$NEW_BROKERS]"

    # Check if brokers were extracted successfully
    if [[ -z "$PREVIOUS_BROKERS" || -z "$NEW_BROKERS" ]]; then
      echo "Error: Failed to extract brokers from JSON files for partition $PARTITION. Skipping partition."
      continue
    fi

    # Append to report file
    printf "| %-55s | %-15s | %-15s | %-15s |\n" "$TOPIC" "$PARTITION" "$PREVIOUS_BROKERS" "$NEW_BROKERS" >> "$REPORT_FILE"
  done

  if [[ "$MODE" == "--dry-run" ]]; then
    echo "Dry run mode: Report updated for topic $TOPIC."
    echo "Reassignment JSON file path: $JSON_OUT"
  else
    echo "Starting the reassignment..."
    if [[ -n "$THROTTLE" ]]; then
      "$KAFKA_HOME/bin/kafka-reassign-partitions.sh" \
          --zookeeper "$ZOOKEEPER" \
          --reassignment-json-file "$JSON_OUT" \
          --execute --throttle "$THROTTLE"
    else
      "$KAFKA_HOME/bin/kafka-reassign-partitions.sh" \
          --zookeeper "$ZOOKEEPER" \
          --reassignment-json-file "$JSON_OUT" \
          --execute
    fi

    while ! OUTPUT=$("$KAFKA_HOME/bin/kafka-reassign-partitions.sh" \
        --zookeeper "$ZOOKEEPER" \
        --reassignment-json-file "$JSON_OUT" \
        --verify); do
        echo "$OUTPUT"
        sleep 5s
    done

    echo 'Reassignment complete.'

    # Run preferred replica election
    "$KAFKA_HOME/bin/kafka-preferred-replica-election.sh" \
        --zookeeper "$ZOOKEEPER" \
        --path-to-json-file "$JSON_OUT"
    echo 'Done.'

    echo 'Sleeping until Repartition is complete'

    while true; do
      IS_REASSIGNING=$(/usr/bin/zookeepercli --servers "$ZOOKEEPER" -c get /admin/reassign_partitions 2>&1)
      ZK_EXIT_CODE=$?
      echo "Zookeeper CLI output: $IS_REASSIGNING"
      if [[ "$ZK_EXIT_CODE" -eq 0 ]]; then
        echo ""
        echo "======"
        echo "$TOPIC is still reassigning, sleeping for 15 seconds"
        echo "======"
        echo ""
        echo "$IS_REASSIGNING"
        sleep 15
      elif [[ "$IS_REASSIGNING" == *"node does not exist"* ]]; then
        echo "Node /admin/reassign_partitions does not exist. Assuming reassignment is complete."
        break
      else
        echo "Unexpected error from zookeepercli: $IS_REASSIGNING"
        sleep 15
      fi
    done
  fi

  cd - > /dev/null  # Suppress the output of 'cd -'
done < "$TOPICS_FILE"

# Print the final report
echo -e "+---------------------------------------------------------+-----------------+-----------------+-----------------+" >> "$REPORT_FILE"
echo "Final Reassignment Report:"
cat "$REPORT_FILE"

1. From your bastion host, SSH to kafka.example.com
2. As a superuser in tmux run the following commands:
tmux
sudo /usr/local/kafka/bin/kafka-topics.sh --list --zookeeper kafkazookeeper-
zookeeper.example.com:2181 > topics.txt
3. Press CTRL + b and then d to detach tmux session
4. Check on the results of the script occasionally with
$ tmux attach
