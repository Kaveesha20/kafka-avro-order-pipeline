"""
DLQ Inspector
-------------
Reads and pretty-prints all messages currently on the 'orders_dlq' topic,
from the beginning. Useful for proving the DLQ received failed messages
and for triaging why they failed.
"""
import json
from kafka import KafkaConsumer

DLQ_TOPIC = "orders_dlq"
BOOTSTRAP_SERVERS = "localhost:9092"


def run():
    consumer = KafkaConsumer(
        DLQ_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id="dlq-inspector",
        auto_offset_reset="earliest",
        consumer_timeout_ms=5000,  # stop after 5s of no new messages
        value_deserializer=lambda v: v,
    )

    print(f"Reading DLQ topic '{DLQ_TOPIC}'...\n")
    count = 0
    for msg in consumer:
        record = json.loads(msg.value.decode("utf-8"))
        count += 1
        print(f"--- DLQ record #{count} (offset {msg.offset}) ---")
        print(f"  reason : {record['reason']}")
        print(f"  error  : {record['error']}")
        if record.get("order"):
            print(f"  order  : {record['order']}")
        if record.get("raw_hex"):
            print(f"  raw    : {record['raw_hex'][:60]}...")
        print()

    if count == 0:
        print("No DLQ messages found.")
    else:
        print(f"Total DLQ messages: {count}")


if __name__ == "__main__":
    run()
