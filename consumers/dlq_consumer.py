# consumers/dlq_consumer.py
import json
from kafka import KafkaConsumer
from config import KAFKA_BROKER, TOPICS

"""
Dead Letter Queue Consumer

In production: when any consumer fails to process a message,
it sends that message to the DLQ topic instead of crashing.

This consumer monitors DLQ and can:
- Alert engineers
- Log for debugging
- Retry later

For now: we just log it clearly so you can SEE what failed.
"""

if __name__ == "__main__":
    consumer = KafkaConsumer(
        TOPICS["dlq"],
        bootstrap_servers=KAFKA_BROKER,
        group_id="dlq-monitor-group",
        auto_offset_reset='earliest',
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    )

    print("☠️  DLQ Monitor started - watching dead-letter-news...")
    print("   (Send bad messages to dead-letter-news topic to see them here)\n")

    for message in consumer:
        event = message.value
        print(f"💀 DEAD LETTER EVENT:")
        print(f"   Partition : {message.partition}")
        print(f"   Offset    : {message.offset}")
        print(f"   Event     : {json.dumps(event, indent=2)}\n")