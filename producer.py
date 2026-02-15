"""
Step 2: Produce messages to the 'orders' topic.

Key concepts demonstrated:
  - Messages are key-value pairs (both are bytes)
  - The KEY determines which partition a message goes to
    → Same key = same partition = guaranteed ordering for that key
  - Delivery callbacks confirm where each message landed

Usage:
    python3 producer.py
"""

import json
import random
import time
from confluent_kafka import Producer

BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094"
TOPIC = "orders"

# Sample data
PRODUCTS = ["laptop", "phone", "headphones", "keyboard", "monitor"]
CUSTOMERS = ["alice", "bob", "charlie", "diana", "eve"]


def delivery_callback(err, msg):
    """Called once per message to indicate delivery result."""
    if err:
        print(f"  ❌ Delivery failed: {err}")
    else:
        print(
            f"  ✅ Delivered to partition {msg.partition()} "
            f"at offset {msg.offset()} "
            f"[key={msg.key().decode('utf-8')}]"
        )


def produce_orders(count=20):
    producer = Producer({
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        # Wait for leader acknowledgment (1) or all replicas (all)
        "acks": "all",
    })

    print(f"Producing {count} order events to '{TOPIC}'...\n")

    for i in range(count):
        # Create an order event
        customer = random.choice(CUSTOMERS)
        order = {
            "order_id": i + 1,
            "customer": customer,
            "product": random.choice(PRODUCTS),
            "quantity": random.randint(1, 5),
            "price": round(random.uniform(10, 500), 2),
            "timestamp": time.time(),
        }

        # KEY determines the partition
        # Same customer → same partition → ordered per customer
        key = customer
        value = json.dumps(order)

        print(f"Sending order #{order['order_id']} for {customer}...")

        producer.produce(
            topic=TOPIC,
            key=key,
            value=value,
            callback=delivery_callback,
        )

        # Trigger delivery callbacks (non-blocking)
        producer.poll(0)

        time.sleep(0.3)  # Slow down so you can watch it happen

    # Wait for all messages to be delivered
    remaining = producer.flush(timeout=10)
    print(f"\n{'='*50}")
    print(f"Done! {count - remaining}/{count} messages delivered.")
    print(f"\n💡 Notice how the same customer always goes to the same partition!")


if __name__ == "__main__":
    produce_orders()
