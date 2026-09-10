# Kafka Avro Order Processing Pipeline

This project is a Kafka-based system for producing and consuming order messages using Avro serialization. It supports real-time aggregation (running average price), retry logic for temporary failures, and a Dead Letter Queue (DLQ) for messages that can't be processed.

## What it does

- A producer generates order messages (`orderId`, `product`, `price`), serializes them with Avro, and sends them to a Kafka topic called `orders`.
- A consumer reads from `orders`, decodes the Avro messages, and keeps a running average price per product as messages come in.
- If a message fails to deserialize (bad/corrupt Avro), it's sent straight to a DLQ topic (`orders_dlq`) — retrying doesn't help here since the bytes themselves are broken.
- If a message deserializes fine but fails a business rule (e.g. negative price), the consumer retries it a few times before giving up and sending it to the DLQ.
- A small script (`dlq_inspector.py`) lets you read back everything that ended up in the DLQ.

## Why Avro instead of JSON

The assignment required Avro, but even without that requirement it makes sense here: Kafka topics stay around for a long time and get read by consumers independently of when producers change. JSON doesn't enforce any structure, so a typo in a field name just becomes a silent bug somewhere downstream. Avro forces both sides to agree on a schema ahead of time, which also makes the messages smaller since field names aren't repeated in every message.

I used a plain `.avsc` file instead of a Schema Registry since the assignment scope didn't need it — this is simpler to set up, but it means the producer and consumer both have to manually stay in sync on the schema file.

## Project structure

```
kafka-avro-order-pipeline/
├── README.md
├── requirements.txt
├── schemas/
│   └── order.avsc
├── producer/
│   └── producer.py
├── consumer/
│   └── consumer.py
└── dlq/
    └── dlq_inspector.py
```

## Setup

You need Python 3.9+ and a Kafka broker running on `localhost:9092`. I ran Kafka locally with Docker:

```bash
docker run -p 9092:9092 apache/kafka:latest
```

Install the Python dependencies:

```bash
pip install -r requirements.txt
```

Kafka is set to auto-create topics, so `orders` and `orders_dlq` get created automatically the first time a message is sent — no need to create them manually.

## Running it

Start the consumer first and leave it running:

```bash
cd consumer
python consumer.py
```

Then, in a separate terminal, run the producer:

```bash
cd producer
python producer.py
```

You'll see the consumer print out each message it receives and the updated running average for that product.

## Testing the failure handling

There are two flags on the producer to trigger the failure paths:

**Malformed Avro message:**
```bash
python producer.py --count 10 --inject-bad-avro
```
This sends one message that isn't valid Avro at all. The consumer fails to deserialize it and sends it straight to the DLQ — no retry, since retrying wouldn't fix corrupt bytes.

**Valid message, invalid data (negative price):**
```bash
python producer.py --count 10 --inject-negative-price
```
This message deserializes fine but fails the price validation check. The consumer retries it 3 times with a short backoff before sending it to the DLQ.

Both of these can be run against the same consumer session — it doesn't need to be restarted between runs, since it just keeps listening on the topic the whole time.

**To check what ended up in the DLQ:**
```bash
cd dlq
python dlq_inspector.py
```
This prints out every message currently in `orders_dlq`, along with why it failed (`malformed_avro` or `processing_failure`).

## A few design notes

- Malformed messages skip the retry loop entirely — if the bytes are broken, retrying doesn't change that, so it goes straight to DLQ. Business-rule failures (like a negative price) get a few retry attempts first, since in a real system that kind of failure could sometimes be transient.
- The running average is just kept in memory (a dictionary keyed by product) — it resets if the consumer restarts. A more production-ready version would persist this somewhere.
- DLQ messages are stored as JSON rather than Avro, mainly so they're easy to read directly and because a malformed message might not even be valid Avro to begin with.
