"""
Read messages from a topic within a specific date/time range.

Uses Kafka's time index to jump directly to the start time,
then reads until the end time.

Usage:
    # Read messages from the last 30 minutes (default)
    python3 consumer_date_range.py

    # Read messages from a specific range
    python3 consumer_date_range.py --start "2026-02-15 10:00:00" --end "2026-02-15 11:00:00"

    # Read everything from the last 2 hours
    python3 consumer_date_range.py --last-hours 2
"""

import argparse
import json
import datetime
from confluent_kafka import Consumer, TopicPartition, KafkaError

BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094"
TOPIC = "orders"


def get_partition_count(topic):
    """Get the number of partitions for a topic."""
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "date-range-helper",
    })
    metadata = consumer.list_topics(topic, timeout=10)
    count = len(metadata.topics[topic].partitions)
    consumer.close()
    return count


def consume_date_range(start_dt, end_dt):
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "group.id": "date-range-reader",
        "enable.auto.commit": False,  # Don't commit — we're just reading
    })

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    print(f"Reading messages from '{TOPIC}'")
    print(f"  Start: {start_dt}")
    print(f"  End:   {end_dt}")
    print(f"{'='*70}\n")

    # Get partition count and create timestamp-based partition list
    num_partitions = get_partition_count(TOPIC)
    partitions = [
        TopicPartition(TOPIC, p, start_ms)
        for p in range(num_partitions)
    ]

    # Ask Kafka: "what offset corresponds to this timestamp?"
    offsets = consumer.offsets_for_times(partitions)

    # Check if any partition has data in this range
    valid_partitions = [tp for tp in offsets if tp.offset > 0]
    if not valid_partitions:
        print("No messages found in this time range.")
        consumer.close()
        return

    # Assign consumer to those offsets
    consumer.assign(offsets)

    print(f"{'Partition':<12} {'Offset':<10} {'Timestamp':<22} {'Key':<12} {'Order'}")
    print("-" * 90)

    message_count = 0

    try:
        while True:
            msg = consumer.poll(timeout=2.0)

            if msg is None:
                break  # No more messages

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"Error: {msg.error()}")
                break

            # Check timestamp — stop if past end time
            msg_timestamp_ms = msg.timestamp()[1]
            if msg_timestamp_ms > end_ms:
                continue  # Skip messages past end time (other partitions may still have valid ones)

            key = msg.key().decode("utf-8") if msg.key() else "N/A"
            value = json.loads(msg.value().decode("utf-8"))
            msg_time = datetime.datetime.fromtimestamp(msg_timestamp_ms / 1000)
            message_count += 1

            print(
                f"P{msg.partition():<11} {msg.offset():<10} "
                f"{msg_time.strftime('%Y-%m-%d %H:%M:%S'):<22} "
                f"{key:<12} "
                f"Order #{value['order_id']}: "
                f"{value['quantity']}x {value['product']} "
                f"(${value['price']})"
            )

    finally:
        consumer.close()
        print(f"\n{'='*70}")
        print(f"Total messages in range: {message_count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Read Kafka messages by date range")
    parser.add_argument("--start", help="Start datetime (e.g. '2026-02-15 10:00:00')")
    parser.add_argument("--end", help="End datetime (e.g. '2026-02-15 11:00:00')")
    parser.add_argument("--last-hours", type=float, help="Read messages from the last N hours")
    parser.add_argument("--last-minutes", type=float, help="Read messages from the last N minutes")

    args = parser.parse_args()

    now = datetime.datetime.now()

    if args.last_hours:
        start = now - datetime.timedelta(hours=args.last_hours)
        end = now
    elif args.last_minutes:
        start = now - datetime.timedelta(minutes=args.last_minutes)
        end = now
    elif args.start and args.end:
        start = datetime.datetime.strptime(args.start, "%Y-%m-%d %H:%M:%S")
        end = datetime.datetime.strptime(args.end, "%Y-%m-%d %H:%M:%S")
    else:
        # Default: last 30 minutes
        start = now - datetime.timedelta(minutes=30)
        end = now

    consume_date_range(start, end)
