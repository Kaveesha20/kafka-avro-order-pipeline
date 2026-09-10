# Kafka Avro Order Processing Pipeline

A Kafka pipeline that produces and consumes **Avro-serialized** order events,
maintains a **running average** of order amounts per customer, and handles
failures gracefully with **retry logic** and a **Dead Letter Queue (DLQ)**.

## Architecture

```
producer.py --(Avro bytes)--> [orders topic] --> consumer.py --> running avg per customer
                                                       |
                                          (malformed / failed after retries)
                                                       v
                                              [orders_dlq topic] --> dlq_inspector.py
```

- **Schema**: `schemas/order.avsc` — a local Avro schema file (no
  Schema Registry), matching the assignment's required fields: `orderId`
  (string), `product` (string), `price` (float). Producer and consumer both
  read this file directly and must stay in sync manually.
- **Producer**: generates fake order messages, serializes them to Avro
  binary with `fastavro`, and publishes to the `orders` topic.
- **Consumer**: deserializes Avro, validates business rules (price must be
  positive), retries transient failures up to 3 times with backoff, and
  routes unrecoverable messages to `orders_dlq`. Running average price is
  tracked per product.
- **DLQ**: a separate topic holding JSON records with the failure reason
  (`malformed_avro` or `processing_failure`), the error, and the original
  event/raw bytes for triage.

## Why Avro over JSON (short version)

Avro is binary and schema-typed, so messages are smaller and type-safe
compared to JSON's text format and loose typing. In production, a Schema
Registry manages schema versions/compatibility across services; here we use
a local `.avsc` file, which is simpler but requires producer and consumer to
manually stay in sync.

## Project structure

```
kafka-avro-project/
├── README.md
├── schemas/
│   └── order.avsc
├── producer/
│   └── producer.py
├── consumer/
│   └── consumer.py
├── dlq/
│   └── dlq_inspector.py
└── tests/
```

## Prerequisites

- Python 3.9+
- A running Kafka broker on `localhost:9092` (e.g. via Docker:
  `docker run -p 9092:9092 apache/kafka:latest`, or your existing local setup)
- Install dependencies:

```bash
pip install fastavro kafka-python
```

## Setup

Create the two topics (optional — Kafka can auto-create them, but explicit
creation is cleaner for a demo):

```bash
kafka-topics.sh --create --topic orders --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
kafka-topics.sh --create --topic orders_dlq --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

## Running the demo

**Terminal 1 — start the consumer** (leave running):

```bash
cd consumer
python consumer.py
```

**Terminal 2 — run the producer** to send normal traffic:

```bash
cd producer
python producer.py --count 20 --delay 0.5
```

Watch Terminal 1 — you'll see each order deserialized and the running
average per customer update live.

## Testing the DLQ (failure scenarios)

Two failure modes are built into the producer via flags:

**1. Malformed Avro (fails deserialization):**

```bash
python producer.py --count 10 --inject-bad-avro
```

The consumer will hit the corrupt message mid-stream, fail to deserialize
it, and immediately route it to `orders_dlq` with `reason: malformed_avro`
— without crashing or blocking subsequent messages.

**2. Valid Avro but invalid business data (fails processing after retries):**

```bash
python producer.py --count 10 --inject-negative-price
```

The consumer will successfully deserialize this message, but
`validate_business_rules` will reject the negative price. You'll see it
retry 3 times with increasing backoff in Terminal 1, then route it to
`orders_dlq` with `reason: processing_failure`.

**3. Inspect the DLQ** to confirm both failure types landed there:

```bash
cd dlq
python dlq_inspector.py
```

Expected output: two DLQ records — one `malformed_avro`, one
`processing_failure` — each with their error message and (where available)
the original event data.

