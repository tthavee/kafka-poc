"""
Step 4: Consumer Group demo — run multiple instances to see load balancing.

Key concepts demonstrated:
  - All instances share the SAME group.id → Kafka distributes partitions
  - With 3 partitions and 2 consumers → one gets 2 partitions, one gets 1
  - With 3 partitions and 3 consumers → each gets exactly 1 partition
  - With 3 partitions and 4 consumers → one consumer sits idle!
  - Kill a consumer → Kafka REBALANCES partitions to survivors

Usage:
    # Open 3 separate terminals and run in each:
    python3 consumer_group.py

    # Then in another terminal, produce messages:
    python3 producer.py

    # Watch how messages are distributed across consumers!
    # Try killing one consumer (Ctrl+C) and watch rebalancing.
"""

import json
import os
import signal
import sys
import uuid
from confluent_kafka import Consumer, KafkaError

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "orders"

# Give each instance a short unique name for display
INSTANCE_ID = str(uuid.uuid4())[:8]

running = True


def signal_handler(sig, frame):
    global running
    print(f"\n\n[{INSTANCE_ID}] Shutting down...")
    running = False


signal.signal(signal.SIGINT, signal_handler)


def on_assign(consumer, partitions):
    """Called when partitions are assigned to this consumer."""
    parts = [str(p.partition) for p in partitions]
    print(f"\n[{INSTANCE_ID}] 🔄 Assigned partitions: [{', '.join(parts)}]")
    print(f"[{INSTANCE_ID}] Listening...\n")


def on_revoke(consumer, partitions):
    """Called when partitions are revoked (rebalance happening)."""
    parts = [str(p.partition) for p in partitions]
    print(f"\n[{INSTANCE_ID}] ⚠️  Partitions revoked: [{', '.join(parts)}] (rebalancing...)")


def consume_group():
    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        # ALL instances use the SAME group.id → they form a consumer group
        "group.id": "order-processing-group",
        "auto.offset.reset": "latest",  # Only read NEW messages
        "enable.auto.commit": True,
        # Faster rebalance detection for the demo
        "session.timeout.ms": 10000,
        "heartbeat.interval.ms": 3000,
    })

    consumer.subscribe(
        [TOPIC],
        on_assign=on_assign,
        on_revoke=on_revoke,
    )

    print(f"{'='*50}")
    print(f"Consumer Group Instance: {INSTANCE_ID}")
    print(f"Group: order-processing-group")
    print(f"Topic: {TOPIC}")
    print(f"{'='*50}")
    print(f"Waiting for partition assignment...\n")

    message_count = 0

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"[{INSTANCE_ID}] ❌ Error: {msg.error()}")
                continue

            key = msg.key().decode("utf-8") if msg.key() else "N/A"
            value = json.loads(msg.value().decode("utf-8"))
            message_count += 1

            print(
                f"[{INSTANCE_ID}] P{msg.partition()} | offset {msg.offset()} | "
                f"{key} → Order #{value['order_id']}: "
                f"{value['quantity']}x {value['product']}"
            )

    finally:
        consumer.close()
        print(f"\n[{INSTANCE_ID}] Processed {message_count} messages. Goodbye!")


if __name__ == "__main__":
    consume_group()
