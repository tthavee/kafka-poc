"""
Step 3: Basic consumer — reads all messages from the 'orders' topic.

Key concepts demonstrated:
  - auto.offset.reset = 'earliest' → read from the beginning
  - Each message has a partition, offset, key, and value
  - Offsets are Kafka's way of tracking "where you are" in the stream
  - Consumer commits offsets so it can resume where it left off

Usage:
    python3 consumer.py

    Press Ctrl+C to stop.
"""

import json
import signal
import sys
from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"

running = True


def signal_handler(sig, frame):
    global running
    print("\n\nShutting down consumer...")
    running = False


signal.signal(signal.SIGINT, signal_handler)


def consume():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        # A unique group id — this consumer gets its own view of the topic
        "group.id": "basic-consumer-3",
        # Start from the earliest message if no committed offset exists
        "auto.offset.reset": "latest",
        # Auto-commit offsets every 5 seconds (default)
        "enable.auto.commit": True,
    })

    consumer.subscribe([TOPIC])
    print(f"Subscribed to '{TOPIC}'. Waiting for messages...\n")
    print(f"{'Partition':<12} {'Offset':<10} {'Key':<12} {'Order'}")
    print("-" * 70)

    message_count = 0

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # Reached end of partition — not an error
                    print(f"  (Reached end of partition {msg.partition()})")
                else:
                    print(f"  ❌ Error: {msg.error()}")
                continue

            # Parse the message
            key = msg.key().decode("utf-8") if msg.key() else "N/A"
            value = json.loads(msg.value().decode("utf-8"))
            message_count += 1

            print(
                f"P{msg.partition():<11} {msg.offset():<10} {key:<12} "
                f"Order #{value['order_id']}: "
                f"{value['quantity']}x {value['product']} "
                f"(${value['price']})"
            )

    finally:
        consumer.close()
        print(f"\nConsumed {message_count} messages total.")


if __name__ == "__main__":
    consume()
