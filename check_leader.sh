#!/bin/bash

# Set Kafka home directory
KAFKA_HOME="/usr/local/kafka/"

# Zookeeper host
ZOOKEEPER_HOST="zookeeper.example.com"

# Broker ID to check (108 in your case)
BROKER_ID=108

# List of topics that match the pattern
topics=$($KAFKA_HOME/bin/kafka-topics.sh --zookeeper $ZOOKEEPER_HOST --list | grep "example.topic")

if [[ -z "$topics" ]]; then
    echo "No topics found matching 'example.topic'."
    exit 1
fi

echo "Found topics:"
echo "$topics"
echo

# Iterate over each topic
for topic in $topics; do
    echo "Describing topic: $topic"

    # Fetch partition details for each topic
    topic_description=$($KAFKA_HOME/bin/kafka-topics.sh --zookeeper $ZOOKEEPER_HOST --describe --topic "$topic")

    # Debugging: show the topic description fetched
    echo "Topic Description:"
    echo "$topic_description"
    echo

    # Check if describe command returns any result
    if [[ -z "$topic_description" ]]; then
        echo "No description found for topic: $topic"
        continue
    fi

    # Parse the output for each partition to check if broker 108 is the leader or in ISR
    echo "$topic_description" | while read -r line; do
        # Extract partition leader, replicas, and ISR
        if [[ "$line" =~ Partition:\ ([0-9]+)\,.*Leader:\ ([0-9]+)\,.*Replicas:\ ([0-9,]+)\,.*Isr:\ ([0-9,]+) ]]; then
            partition=${BASH_REMATCH[1]}
            leader=${BASH_REMATCH[2]}
            replicas=${BASH_REMATCH[3]}
            isr=${BASH_REMATCH[4]}

            # Check if broker 108 is the leader or in ISR
            if [[ "$leader" == "$BROKER_ID" || ",$isr," == *",$BROKER_ID,"* ]]; then
                echo "Topic: $topic, Partition: $partition, Leader: $leader, Replicas: $replicas, ISR: $isr"
            fi
        fi
    done
done