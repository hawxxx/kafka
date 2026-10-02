# Method of Procedure (MOP) — Kafka Partition Rebalance

## Overview

This MOP covers the rebalancing of Kafka topic partitions across active brokers in a production cluster. The procedure uses the `kafka-rebalance.sh` script to generate a reassignment plan, execute it with throttling (optional), and verify completion.

**Target Environment:** `kafka.example.com`
**Zookeeper:** `zookeeper.example.com:2181`

---

## Pre-Procedure

### Prerequisites

- Confirm maintenance window is scheduled and approved
- Verify network access from your bastion host to the Kafka broker
- Confirm `jq`, `zookeepercli`, and Kafka CLI tools are installed at `/usr/local/kafka/` on the broker
- Verify Zookeeper ensemble is healthy and all brokers are registered
- Confirm sufficient disk space on brokers for partition data movement
- Notify stakeholders (producers/consumers owners) of the upcoming rebalance

### Pre-Checks

1. SSH to the Kafka broker and verify broker health:

```bash
/usr/local/kafka/bin/zookeeper-shell.sh zookeeper.example.com:2181 ls /brokers/ids
```

2. Confirm all expected broker IDs are listed in the output.

3. Verify no existing reassignment is in progress:

```bash
/usr/bin/zookeepercli --servers zookeeper.example.com:2181 -c get /admin/reassign_partitions
```

Expected output: `node does not exist` (no active reassignment).

4. Verify disk utilization on brokers is below 70%:

```bash
df -h /data/kafka/
```

5. Confirm the `kafka-rebalance.sh` script and `topics.txt` are present in the working directory.

---

## Procedure

### Step 1 — Connect to the Kafka Broker, Prod Bastion, or any host with Kafka reachability

From your bastion host, SSH to a broker:

```bash
ssh kafka.example.com
```

### Step 2 — Start a tmux Session

Start a tmux session to ensure the process survives disconnection:

```bash
tmux
```

Elevate to superuser:

```bash
sudo -i
```

### Step 3 — Generate the Topics File

List all topics, filtering out internal/system topics (adjust as needed for specific topics to target), and write to `topics.txt`:

```bash
/usr/local/kafka/bin/kafka-topics.sh --list --zookeeper zookeeper.example.com:2181 \
  | grep -v '^__' \
  | grep -v '_schemas$' \
  | grep 'example' > topics.txt
```

Adjust the `grep` patterns to match the topics that require rebalancing. Review the file before proceeding:

```bash
cat topics.txt
wc -l topics.txt
```

### Step 4 — Dry Run

Generate the reassignment plan without executing:

```bash
./kafka-rebalance.sh zookeeper.example.com:2181 topics.txt --dry-run
```

Review the output in `reassignment_report.txt` to confirm the proposed partition moves are expected.

### Step 5 — Execute the Rebalance with Throttle

Run the reassignment with a 50 MB/s throttle to limit inter-broker replication traffic:

```bash
./kafka-rebalance.sh zookeeper.example.com:2181 topics.txt --throttle 50000000
```

### Step 6 — Detach tmux

Press `Ctrl+b` then `d` to detach from the tmux session. The script continues running in the background.

### Step 7 — Monitor Progress

Reattach to the tmux session periodically to check progress:

```bash
tmux attach
```

The script polls reassignment status automatically. Look for `Reassignment complete.` messages per topic.

### Step 8 — Verify Completion

Once the script finishes, confirm no reassignment is in progress:

```bash
/usr/bin/zookeepercli --servers zookeeper.example.com:2181 -c get /admin/reassign_partitions
```

Expected output: `node does not exist`.

---

## Post-Procedure

1. Review the final reassignment report:

```bash
cat reassignment_report.txt
```

2. Verify partition distribution is balanced across brokers:

```bash
/usr/local/kafka/bin/kafka-topics.sh --zookeeper zookeeper.example.com:2181 --describe --topic <TOPIC>
```

3. Confirm preferred replica election completed (leaders match the first replica in the assignment).

4. Monitor consumer lag for affected topics to ensure consumers are keeping up:

```bash
/usr/local/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka.example.com:9092 --describe --group <GROUP>
```

5. Verify broker disk utilization has stabilized.

6. Notify stakeholders that the rebalance is complete.

7. Retain the `backup_reassignment_plan/` directory until the new assignment is confirmed stable (minimum 24 hours).

---

## Test Procedure

Use this procedure to validate the rebalance process in a non-production environment before executing in production.

1. Identify a staging or test cluster with similar topology.

2. Create a test topic with multiple partitions:

```bash
/usr/local/kafka/bin/kafka-topics.sh --zookeeper <STAGING_ZK>:2181 --create --topic test-rebalance --partitions 12 --replication-factor 3
```

3. Run the dry-run against the test topic:

```bash
echo "test-rebalance" > test-topics.txt
./kafka-rebalance.sh <STAGING_ZK>:2181 test-topics.txt --dry-run
```

4. Verify the report shows expected partition moves.

5. Execute the rebalance with throttle:

```bash
./kafka-rebalance.sh <STAGING_ZK>:2181 test-topics.txt --throttle 50000000
```

6. Confirm reassignment completes and leaders are correctly elected.

7. Test rollback:

```bash
./kafka-rebalance.sh <STAGING_ZK>:2181 test-topics.txt --rollback
```

8. Verify partitions return to their original assignment.

### Validation

9. Login to Grafana and open the **Kafka JMX Stats** dashboard:
   - https://grafana.example.com/
   - Confirm there are no **UnderReplicatedPartitions**. If present, refer to the runbook for the UnderReplicatedPartitions alert.
   - Confirm there are no **OfflinePartitions**.

10. Open your **MQTT service** dashboard and verify normal operation:
    - https://grafana.example.com/
    - Check **Kafka Messages Per Type** volume is consistent with pre-maintenance levels.
    - Check **Kafka Errors Per Type** — confirm no new errors are appearing.

11. Login to Icinga2 and confirm there are no alerts related to the maintenance. If services are alerting, restart them as needed.

---

## Back-Out Procedure

If the rebalance causes issues (increased latency, consumer lag, broker instability), roll back to the previous assignment.

### Step 1 — Stop the Running Script (if still in progress)

Reattach to tmux and press `Ctrl+c` to interrupt the script. The script traps SIGINT for graceful exit.

### Step 2 — Verify Backup Files Exist

```bash
ls backup_reassignment_plan/
```

Confirm `.backup.json` files exist for each topic.

### Step 3 — Execute Rollback

```bash
./kafka-rebalance.sh zookeeper.example.com:2181 topics.txt --rollback
```

The script reads each topic's backup JSON and re-executes the original assignment. It verifies completion per topic before proceeding to the next.

### Step 4 — Verify Rollback Completion

```bash
/usr/bin/zookeepercli --servers zookeeper.example.com:2181 -c get /admin/reassign_partitions
```

Expected output: `node does not exist`.

### Step 5 — Validate Original Assignment Restored

```bash
/usr/local/kafka/bin/kafka-topics.sh --zookeeper zookeeper.example.com:2181 --describe --topic <TOPIC>
```

Compare the output against the backup JSON to confirm partitions are back to their original brokers.

### Step 6 — Manual Rollback (if script rollback fails)

If the `--rollback` flag fails, apply backup files manually per topic:

```bash
/usr/local/kafka/bin/kafka-reassign-partitions.sh \
  --zookeeper zookeeper.example.com:2181 \
  --reassignment-json-file backup_reassignment_plan/<TOPIC>.backup.json \
  --execute
```

Verify:

```bash
/usr/local/kafka/bin/kafka-reassign-partitions.sh \
  --zookeeper zookeeper.example.com:2181 \
  --reassignment-json-file backup_reassignment_plan/<TOPIC>.backup.json \
  --verify
```

### Step 7 — Notify Stakeholders

Inform stakeholders that the rebalance was rolled back and the cluster is restored to its previous state.

---

## Resuming After Interruption

If the script is interrupted mid-execution (network drop, session timeout, manual stop):

1. Reattach to tmux: `tmux attach`
2. Check if a reassignment is still in progress:

```bash
/usr/bin/zookeepercli --servers zookeeper.example.com:2181 -c get /admin/reassign_partitions
```

3. If a reassignment is active, wait for it to complete naturally — Kafka finishes in-progress reassignments regardless of the script state.

4. Once complete, remove already-processed topics from `topics.txt` (topics that show in `reassignment_report.txt` as completed).

5. Re-run the script with the updated `topics.txt` to continue with remaining topics:

```bash
./kafka-rebalance.sh zookeeper.example.com:2181 topics.txt --throttle 50000000
```
