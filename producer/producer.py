"""
Kafka Avro Producer
--------------------
Generates order messages matching the assignment's order.avsc schema
(orderId, product, price), serializes them with Avro, and publishes them
to the 'orders' Kafka topic.

Supports optional flags to inject failure scenarios so the consumer's
retry + DLQ logic can be demonstrated:
  --inject-bad-avro        sends one deliberately corrupt (non-Avro) payload
  --inject-negative-price  sends one valid-Avro message with price < 0
"""
import argparse
import io
import json
import random
import time

from fastavro import schemaless_writer, parse_schema
from kafka import KafkaProducer

SCHEMA_PATH = "../schemas/order.avsc"
TOPIC = "orders"
BOOTSTRAP_SERVERS = "localhost:9092"

PRODUCTS = ["Item1", "Item2", "Item3", "Item4", "Item5"]


def load_schema(path=SCHEMA_PATH):
    with open(path, "r") as f:
        return parse_schema(json.load(f))


def make_order(order_num, force_negative_price=False):
    """Build one order event as a dict matching order.avsc."""
    price = round(random.uniform(10, 500), 2)
    if force_negative_price:
        price = -price  # deliberately invalid -> processing failure downstream

    return {
        "orderId": str(1000 + order_num),
        "product": random.choice(PRODUCTS),
        "price": price,
    }


def serialize_avro(event, schema):
    buf = io.BytesIO()
    schemaless_writer(buf, schema, event)
    return buf.getvalue()


def build_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v,  # we pre-serialize to Avro bytes ourselves
    )


def run(num_messages, delay, inject_bad_avro, inject_negative_price):
    schema = load_schema()
    producer = build_producer()

    print(f"Producing {num_messages} messages to topic '{TOPIC}'...")

    for i in range(num_messages):
        # Scenario 1: malformed Avro bytes (not valid Avro at all)
        if inject_bad_avro and i == num_messages // 2:
            bad_payload = b"this-is-not-valid-avro-binary-data"
            producer.send(TOPIC, value=bad_payload)
            print(f"[{i}] Sent INTENTIONALLY CORRUPT (non-Avro) message to trigger DLQ")
            continue

        # Scenario 2: valid Avro, but invalid business data (negative price)
        force_negative = inject_negative_price and i == num_messages // 3
        event = make_order(i, force_negative_price=force_negative)
        payload = serialize_avro(event, schema)
        producer.send(TOPIC, value=payload)

        tag = " (negative price -> will fail processing)" if force_negative else ""
        print(f"[{i}] Sent order {event['orderId']} product={event['product']} price={event['price']}{tag}")

        time.sleep(delay)

    producer.flush()
    producer.close()
    print("Done producing.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Avro order message producer")
    parser.add_argument("--count", type=int, default=20, help="Number of messages to send")
    parser.add_argument("--delay", type=float, default=0.5, help="Delay between messages (sec)")
    parser.add_argument("--inject-bad-avro", action="store_true",
                         help="Send one deliberately corrupt (non-Avro) message to test DLQ")
    parser.add_argument("--inject-negative-price", action="store_true",
                         help="Send one valid-Avro message with a negative price to test DLQ")
    args = parser.parse_args()

    run(args.count, args.delay, args.inject_bad_avro, args.inject_negative_price)
