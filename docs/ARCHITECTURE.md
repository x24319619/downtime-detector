# DOWNTIME_DETECTOR — Architecture

**Project:** DOWNTIME_DETECTOR — Website Downtime & Error Monitor
**Module:** MSc Cloud Computing — Scalable Cloud Programming (MSCCLOUD_JAN26BI)
**Team:** Sonal Tyagi (X24319619) · Vishwas Dubey (X24197360)

---

## 1. Problem & Use Case

**The real-time question this system answers:**
*Which endpoints are currently degraded compared to their historical norm — i.e. a genuine incident, not normal background noise?*

A simple error-count alert (e.g. "endpoint X had 50 errors") is not useful on its own — an
endpoint that normally gets 1000 requests/minute with a typical 2% error rate having 50
errors in 5 minutes is _normal_. The same 50 errors on a low-traffic endpoint that
normally has near-zero errors is an _incident_. Answering this properly requires:

- A **live view** of what's happening right now (last 5 minutes)
- A **historical baseline** of what "normal" looks like for that specific endpoint

Neither view alone answers the question — which is the justification for a **Lambda
architecture** (batch + speed) rather than a batch-only or stream-only design.

## 2. Dataset

**Source:** [Web Server Access Logs](https://www.kaggle.com/datasets/eliasdabbas/web-server-access-logs) — Kaggle
(originally published as Zaker, Farzin, 2019, *"Online Shopping Store - Web Server
Logs,"* Harvard Dataverse, V1, https://doi.org/10.7910/DVN/3QBYB5)

This is a **real (not synthetic)** access log dataset from an actual
e-commerce website (zanbil.ir). Every line is a real visitor
request — product page views, searches, cart/checkout activity, image loads, etc. —
so /checkout, /product/..., /cart and similar paths are genuine endpoints of a
live online store, giving a realistic, business-relevant "is this endpoint broken
right now" story (an e-commerce checkout page going down is a genuine, high-stakes
incident in the real world).

Format: raw Apache Combined Log Format (extended with a trailing response-time
field where present), e.g.:

```
233.223.117.90 - - [27/Dec/2037:12:00:00 +0530] "DELETE /usr/admin HTTP/1.0" 502 4963 "-" "Mozilla/5.0 ..." 45
```

Fields extracted per record:

| Field | Description |
|---|---|
| `client_ip` | Requesting client's IP address |
| `timestamp` | Original log timestamp (Apache format, in brackets) |
| `request_type` | HTTP method — GET, POST, PUT, DELETE |
| `endpoint` | Requested path (e.g. `/checkout`, `/product/123`) |
| `status_code` | HTTP response status (200, 404, 500, 502, etc.) |
| `bytes_sent` | Response size in bytes |
| `referrer` | Referring URL, or `-` if none |
| `response_time` | Server response time in ms (trailing field, where present) |
| `ingested_at` / `ingested_epoch` | Stamped by the producer at send time — this is what the speed layer windows on, since it reflects the actual replay time rather than the log's original historical timestamp |

### 2.1 75/25 split methodology

Rather than replaying the entire file through Kinesis, the dataset is deliberately
split by line order into two parts before ingestion:

- **First 75% of lines → `batch_master_data.log`.** Uploaded directly to S3 (no
  Kinesis involved). This is the batch layer's "master data" — the full historical
  record the EMR/PySpark job reads to compute each endpoint's baseline error rate
  and average response time.
- **Last 25% of lines → `speed_layer_stream.log`.** Replayed through
  `02_producer.py` into Kinesis at a controlled, configurable rate, to simulate this
  slice of the timeline arriving as live traffic. This is the only portion the speed
  layer (Lambda + DynamoDB) ever sees in real time.

This split is what makes the batch layer's baseline genuinely independent of the
"live" data being evaluated against it — the speed layer's incoming records are
compared against a baseline computed purely from prior, separate history, rather
than baseline and live data being mixed together.

**Append-back (growing the master dataset over time):** in addition to being
processed by the speed layer, the raw records from the 25% "live" slice are also
archived into S3 via **Kinesis Data Firehose**, appended alongside
`batch_master_data.log`. This means that if the batch job is re-run later, it runs
over the full accumulated history (75% + whatever has streamed in since), which
mirrors how a real production Lambda architecture behaves — today's live data
becomes tomorrow's historical baseline.


## 3. System Architecture (Lambda Architecture)

```
Web Server Access Logs (Kaggle, ~3.3GB real e-commerce traffic)
        │  05_split_log_data.py — split 75% / 25% by line order
        │
        ├─── 75% ──────────────────────┐
        │                              ▼
        │                    Uploaded directly to S3
        │                    batch_master_data.log
        │                    (no Kinesis involved)
        │
        └─── 25% ──────────────────────┐
                                        ▼
                          Python boto3 Producer (02_producer.py)
                     reads log lines → regex-parses → JSON → put_record()
                     replayed at a controlled, configurable rate
                                        │
                                        ▼
                     AWS Kinesis Data Streams ("server-pulse-stream", 2 shards)
                     feeds both consumers in parallel from the same stream
                                        │
                    ┌───────────────────┼───────────────────┐
                    ▼ (speed path)      ▼ (append path)      ▼ (batch path, via S3)
┌───────────────────────────┐  ┌──────────────────┐   ┌────────────────────────────────┐
│ AUTO-SCALING BOUNDARY:                                                                │
│ EMR Managed Scaling (batch) + Lambda concurrency (speed)                              │
│                                                                                         │
│  SPEED LAYER                  Kinesis Firehose        BATCH LAYER                      │
│  AWS Lambda                   appends raw records      EMR + PySpark                   │
│  per-record trigger,          to S3, growing           reads batch_master_data.log     │
│  avg error rate/response      batch_master_data.log    (+ appended records) →          │
│  time per endpoint,           over time                historical error rate + avg     │
│  5-min sliding window                                   response time per endpoint       │
│         │                            │                          │                     │
│         ▼                            └──────────────────────────┤                     │
│  DynamoDB                                                        ▼                     │
│  per-endpoint, per-window                              S3 — Parquet                    │
│  error counts, TTL set                                  historical baseline per          │
│                                                          endpoint, Athena-queryable      │
└───────────────────────────────────────────────────────────────────────────────────────┘
        │                                                          │
        └───────────────────────────┬──────────────────────────────┘
                                     ▼ merge
                      SERVING LAYER — AWS Athena + Lambda merge
        joins live error count (DynamoDB) with historical baseline (S3/Athena);
        flags an endpoint "degraded" if current error rate exceeds baseline
        by a set threshold
                                     │
                                     ▼
                      OUTPUT — Alert list + benchmark graphs
        list of degraded endpoints · speedup vs worker count ·
        latency vs ingestion rate · throughput over time
```

### 3.1 What each layer answers

| Layer | Question it answers                                                            |
|---|--------------------------------------------------------------------------------|
| **Speed** | Which endpoints have had more than N error responses in the last 5 minutes?    |
| **Batch** | What is the historical average error rate and response time for each endpoint? |
| **Merge (Serving)** | Which endpoints are currently degraded compared to their historical norm?      |

### 3.2 Component detail

**Ingestion — `02_producer.py`**
Parses each Apache log line with a regex into the canonical schema above, stamps it
with the actual send time, and calls `kinesis.put_record()`. Runs only against the
25% "live" slice (`speed_layer_stream.log`), never the full dataset. Rate and jitter
are configurable via CLI flags (`--rate`, `--jitter`) — this is also the mechanism
used in Phase 3 to vary ingestion rate for benchmarking.

**Dataset split — `05_split_log_data.py`**
Splits the ~3.3GB source file by line order into `batch_master_data.log` (first 75%)
and `speed_layer_stream.log` (last 25%), per the module lecturer's guidance. Run
once, before ingestion begins.

**Speed Layer — Lambda + DynamoDB** *(Vishwas)*
Lambda is triggered directly by the Kinesis event source mapping (batch size 100,
starting position LATEST). Each invocation buckets records into 1-minute windows per
endpoint and atomically increments `error_count`/`total_count` in DynamoDB via
`UpdateItem ADD`. The "5-minute sliding window" view is computed at query time by
summing the last 5 one-minute buckets — a tumbling-bucket-under-the-hood design that
behaves as a sliding window to any caller (see comments in `04_lambda_speed_layer.py`
for the full reasoning). DynamoDB TTL expires buckets after 1 hour.

**Append path — Kinesis Firehose → S3**
A second, independent consumer of the same Kinesis stream. Firehose batches and
appends the raw records from the 25% "live" slice into S3 alongside
`batch_master_data.log`, so the master dataset grows over time rather than staying
fixed at its original 75%. This doesn't compete with or slow down Vishwas's Lambda,
since Firehose and the Lambda event source mapping are two separate, independent
reads of the same stream.

**Batch Layer — EMR + PySpark** *(Sonal)*
Processes `batch_master_data.log` (the original 75% plus anything appended via
Firehose since) to compute, per endpoint: historical average error rate and average
response time. Output is written to S3 in Parquet format, queryable via Athena. This
is the "accurate, comprehensive view over full history" side of the Lambda
architecture — correctness over freshness.

**Serving Layer — Athena + Lambda merge**
Joins the live DynamoDB counts with the S3/Athena historical baseline per endpoint.
An endpoint is flagged "degraded" when its current error rate exceeds its historical
baseline by a defined threshold (e.g. 2x the historical average, tunable). This merge
is what makes the system's core question answerable — neither layer alone can say
"is this abnormal for *this specific* endpoint."

**Auto-scaling boundary**
- EMR: managed scaling on the batch cluster, scaling worker node count based on YARN
  memory pressure
- Lambda: concurrency scales automatically with Kinesis shard count/incoming record
  volume (native Lambda-Kinesis behavior, no manual policy needed beyond the shard
  count itself)

*(Exact scaling triggers/cooldowns/thresholds to be finalized and documented here once configured in Phase 1 — see the AWS Setup Guide.)*

## 4. Why Lambda Architecture (not batch-only or stream-only)

- **Stream-only** would give fast alerts but no sense of "normal" per endpoint — every
  endpoint would need a manually hardcoded threshold, which doesn't generalize and
  would flag naturally high-traffic endpoints constantly.
- **Batch-only** would give an accurate historical picture but with no timeliness —
  by the time a batch job re-runs, an active incident could have already resolved
  itself or gotten much worse.
- **Batch + speed (Lambda architecture)** gives both: correctness from the batch
  layer's full-history view, and freshness from the speed layer's live window — merged
  into a single "is this degraded right now, relative to what's normal for it"
  decision.

## 5. Team Roles

| Person | Owns |
|---|---|
| **Sonal** | AWS setup (Kinesis, IAM/VPC notes), producer script, batch layer (EMR + PySpark), benchmark graph generation, GitHub finalization |
| **Vishwas** | DynamoDB schema, Lambda speed layer, Athena merge/serving layer, leads IEEE report |
| **Both** | End-to-end pipeline testing, benchmarking, demo video, final rubric check |

See `docs/01_aws_setup_guide.md` for step-by-step AWS setup and `README.md` for repo
structure and how to run each component.
