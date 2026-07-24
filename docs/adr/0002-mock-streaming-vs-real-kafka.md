# ADR 0002: Simulate the event stream with a Python generator instead of real Kafka/Pub-Sub

## Status
Accepted

## Context
A production version of this pipeline would ingest from a real message broker
(Kafka, GCP Pub/Sub, AWS Kinesis). This is a portfolio project built and run locally/in
CI with no budget for managed broker infrastructure and no real POS system to connect to.

## Decision
`bronze/producer.py` is a Python generator that emits synthetic sales and customer-profile
events at a configurable rate, writes them out in micro-batches, and lands them as Parquet
files in `data_lake/bronze/` (or a MinIO bucket when run via `docker-compose`). It is
deliberately written so the **only** thing that would change to point this at a real
broker is the ingestion adapter:

- The producer already emits one JSON-serializable event object at a time through a
  Python generator/iterator interface (`for event in generate_events(...)`).
- Swapping to real Kafka means replacing the `write_batch_to_parquet()` sink in
  `producer.py` with a `KafkaProducer.send()` call, and replacing the Bronze *reader*
  (currently "list Parquet files in the landing directory") with a Kafka consumer that
  writes each polled batch to Parquet the same way Bronze does today. No changes are
  needed downstream of Bronze — Silver only ever reads landed Parquet files and doesn't
  know or care whether they arrived via Kafka Connect, a Pub/Sub push subscription, or
  this local generator.
- `docker-compose.yml` runs a MinIO container as an S3-compatible object store so the
  Bronze landing zone behaves like real cloud storage (bucket, prefixes, `s3a://`-style
  paths) rather than a bare local directory, minimizing the delta to a real deployment.

## Alternatives considered
- **Run real Kafka locally via Docker**: considered, but adds a JVM + Zookeeper/KRaft
  dependency that's heavy for a portfolio repo a reviewer wants to `git clone && run`
  in a few minutes, without changing the actual lesson being demonstrated (Bronze
  ingestion contracts, not broker operations).
- **Skip the streaming framing and just write a batch-load script**: rejected because it
  would misrepresent the source system — real POS data is event-shaped and
  continuously-arriving, and Silver's dedup/late-arrival handling only makes sense in that
  context.

## Consequences
- A reviewer can run the entire pipeline with `docker-compose up` and Python, no cloud
  account or broker license needed.
- The "swap to real Kafka" story is documented here rather than proven in code — an
  honest limitation, called out explicitly rather than implied.
