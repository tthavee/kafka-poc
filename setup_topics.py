"""
Step 1: Create Kafka topics with partitions.

This script creates an 'orders' topic with 3 partitions.
Partitions are the unit of parallelism in Kafka — more partitions
allow more consumers to read in parallel.

Usage:
    python3 setup_topics.py
"""

from confluent_kafka.admin import AdminClient, NewTopic

BOOTSTRAP_SERVERS = "localhost:9092"

def create_topics():
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    topics = [
        NewTopic(
            topic="orders_2",
            num_partitions=3,
            replication_factor=1,
        ),
    ]

    print("Creating topics...")
    futures = admin.create_topics(topics)

    for topic, future in futures.items():
        try:
            future.result()  # Block until topic is created
            print(f"  ✅ Topic '{topic}' created successfully")
        except Exception as e:
            print(f"  ⚠️  Topic '{topic}': {e}")

    # List all topics to confirm
    metadata = admin.list_topics(timeout=10)
    print("\nAll topics on the cluster:")
    for topic in sorted(metadata.topics):
        if not topic.startswith("_"):  # Skip internal topics
            partitions = metadata.topics[topic].partitions
            print(f"  📦 {topic} ({len(partitions)} partitions)")


if __name__ == "__main__":
    create_topics()