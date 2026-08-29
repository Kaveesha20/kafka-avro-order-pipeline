"""
Kafka Avro Consumer
--------------------
Consumes Avro-serialized order messages (orderId, product, price) from the
'orders' topic, maintains a running average price per product, and
demonstrates:

  1. Retry logic - transient processing failures are retried up to
     MAX_RETRIES times with backoff before giving up.
  2. Dead Letter Queue (DLQ) - messages that fail Avro deserialization
     (malformed) OR fail business validation after all retries are
     exhausted (processing failure) are sent to the 'orders_dlq' topic
     instead of crashing the consumer or being silently dropped.
"""
import io
import json
import time
from collections import defaultdict

from fastavro import schemaless_reader, parse_schema
from kafka import KafkaConsumer, KafkaProducer

SCHEMA_PATH = "../schemas/order.avsc"
TOPIC = "orders"
DLQ_TOPIC = "orders_dlq"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "order-processing-group"

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 1  # multiplied by attempt number for simple backoff

# Running average state: product -> {"count": int, "total": float}
running_stats = defaultdict(lambda: {"count": 0, "total": 0.0})


def load_schema(path=SCHEMA_PATH):
    with open(path, "r") as f:
        return parse_schema(json.load(f))


def deserialize_avro(raw_bytes, schema):
    """Raises an exception if raw_bytes is not valid Avro for this schema."""
    buf = io.BytesIO(raw_bytes)
    return schemaless_reader(buf, schema)


def validate_business_rules(order):
    """
    Simulates a downstream processing failure, e.g. a business-rule check
    or a database write that could legitimately fail. Raises ValueError
    for invalid data so the retry/DLQ path can be exercised.
    """
    if order["price"] <= 0:
        raise ValueError(f"Invalid price {order['price']} for order {order['orderId']}")
    return True


def update_running_average(order):
    """Update and print the running average price for this product."""
    stats = running_stats[order["product"]]
    stats["count"] += 1
    stats["total"] += order["price"]
    avg = stats["total"] / stats["count"]
    print(
        f"  -> {order['product']}: running avg price = {avg:.2f} "
        f"(n={stats['count']}, last order={order['price']})"
    )
    return avg


def process_with_retry(order, dlq_producer):
    """
    Attempts to process a successfully-deserialized order, retrying on
    failure up to MAX_RETRIES times. If all retries fail, the raw order
    is sent to the DLQ with failure metadata.
    """
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            validate_business_rules(order)
            update_running_average(order)
            return True
        except ValueError as e:
            last_error = str(e)
            print(f"  [retry {attempt}/{MAX_RETRIES}] processing failed: {last_error}")
            if attempt < MAX_RETRIES:
                time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    # Exhausted retries -> send to DLQ
    send_to_dlq(
        dlq_producer,
        reason="processing_failure",
        error=last_error,
        original_order=order,
    )
    return False


def send_to_dlq(dlq_producer, reason, error, original_order=None, raw_bytes=None):
    """
    Publishes a failure record to the DLQ topic as JSON (human-readable,
    easy to inspect/replay later). Includes the failure reason and error
    detail so the DLQ can be triaged.
    """
    dlq_record = {
        "reason": reason,          # "malformed_avro" or "processing_failure"
        "error": error,
        "order": original_order,
        "raw_hex": raw_bytes.hex() if raw_bytes else None,
        "failed_at": time.time(),
    }
    dlq_producer.send(DLQ_TOPIC, value=json.dumps(dlq_record).encode("utf-8"))
    dlq_producer.flush()
    print(f"  !! Sent to DLQ (reason={reason}): {error}")


def run():
    schema = load_schema()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        value_deserializer=lambda v: v,  # raw bytes; we handle Avro decoding ourselves
    )
    dlq_producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: v,
    )

    print(f"Listening on topic '{TOPIC}'... (Ctrl+C to stop)\n")

    for msg in consumer:
        raw_bytes = msg.value
        print(f"Received message at offset {msg.offset} ({len(raw_bytes)} bytes)")

        # Step 1: Avro deserialization - malformed messages go straight to DLQ
        try:
            order = deserialize_avro(raw_bytes, schema)
        except Exception as e:
            send_to_dlq(
                dlq_producer,
                reason="malformed_avro",
                error=str(e),
                raw_bytes=raw_bytes,
            )
            continue

        # Step 2: business processing with retry -> DLQ on exhausted retries
        process_with_retry(order, dlq_producer)


if __name__ == "__main__":
    run()
