# DOWNTIME_DETECTOR — Architecture

**Project:** DOWNTIME_DETECTOR — Website Downtime & Error Monitor
**Module:** MSc Cloud Computing — Scalable Cloud Programming (MSCCLOUD_JAN26AI)
**Team:** Sonal Tyagi (X24319619) · Vishwas Dubey (X24197360)

---

## 1. Problem & Use Case

**The real-time question this system answers:**
*"Which endpoints are currently degraded compared to their historical norm — i.e. a genuine incident, not normal background noise?"*

A simple error-count alert (e.g. "endpoint X had 50 errors") is not useful on its own — an
endpoint that normally gets 1000 requests/minute with a typical 2% error rate having 50
errors in 5 minutes is _normal_. The same 50 errors on a low-traffic endpoint that
normally has near-zero errors is an _incident_. Answering this properly requires:

- A **live view** of what's happening right now (last 5 minutes)
- A **historical baseline** of what "normal" looks like for that specific endpoint

Neither view alone answers the question — which is the justification for a **Lambda
architecture** (batch + speed) rather than a batch-only or stream-only design.

## 2. Dataset

**Source:** [Apache Server Logs (Synthetic)](https://kaggle.com/datasets/vishnu0399/server-logs) — Kaggle

Format: raw Apache Combined Log Format (extended with a trailing response-time field), e.g.:

```
233.223.117.90 - - [27/Dec/2037:12:00:00 +0530] "DELETE /usr/admin HTTP/1.0" 502 4963 "-" "Mozilla/5.0 ..." 45
```

Fields extracted per record:

| Field | Description |
|---|---|
| `client_ip` | Requesting client's IP address |
| `timestamp` | Original log timestamp (Apache format, in brackets) |
| `request_type` | HTTP method — GET, POST, PUT, DELETE |
| `endpoint` | Requested path (e.g. `/usr/admin`) |
| `status_code` | HTTP response status (200, 404, 500, 502, etc.) |
| `bytes_sent` | Response size in bytes |
| `referrer` | Referring URL, or `-` if none |
| `response_time` | Server response time in ms (trailing field, this dataset only) |
| `ingested_at` / `ingested_epoch` | Stamped by the producer at send time — this is what the speed layer windows on, since it reflects the actual replay time rather than the log's original (2037!) timestamp |

This is a static dataset **replayed at a controlled, configurable rate** to simulate a
continuous live stream — an approach explicitly permitted by the CA brief
("Replayed log or clickstream data... replay it at a controlled rate to simulate a
live stream"). This is a deliberate design choice, not a limitation: it gives us
repeatable, comparable benchmark runs (same data, varying only the injection rate),
which is exactly what Phase 3's performance measurement needs.

## 3. System Architecture (Lambda Architecture)

```
Apache Server Log Dataset (Kaggle .log file)
        │  replayed at controlled rate
        ▼
Python boto3 Producer  (02_producer.py)
  reads log lines → regex-parses fields → JSON → puts records into Kinesis
        │
        ▼
AWS Kinesis Data Streams  ("server-pulse-stream", 2 shards)
  feeds both layers in parallel from the same stream
        │
        ├────────────────────────────┬─────────────────────────────┐
        ▼ (speed path)                                ▼ (batch path)
┌───────────────────────────┐        ┌────────────────────────────────┐
│  AUTO-SCALING BOUNDARY: EMR Managed Scaling (batch) + Lambda concurrency (speed) │
│                                                                        │
│  SPEED LAYER                        BATCH LAYER                       │
│  AWS Lambda                         EMR + PySpark                     │
│  per-record trigger,                full log history →                │
│  counts 4xx/5xx per endpoint        historical error rate + avg       │
│  in a 5-min sliding window          response time per endpoint         │
│         │                                    │                        │
│         ▼                                    ▼                        │
│  DynamoDB                           S3 — Parquet                      │
│  per-endpoint, per-window           historical baseline per            │
│  error counts, TTL set              endpoint, Athena-queryable         │
└───────────────────────────┘        └────────────────────────────────┘
        │                                       │
        └───────────────────┬───────────────────┘
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

| Layer | Question it answers |
|---|---|
| **Speed** | "Which endpoints have had more than N error responses in the last 5 minutes?" |
| **Batch** | "What is the historical average error rate and response time for each endpoint?" |
| **Merge (Serving)** | "Which endpoints are currently degraded compared to their historical norm?" |

### 3.2 Component detail

**Ingestion — `02_producer.py`**
Parses each Apache log line with a regex into the canonical schema above, stamps it
with the actual send time, and calls `kinesis.put_record()`. Rate and jitter are
configurable via CLI flags (`--rate`, `--jitter`) — this is also the mechanism used
in Phase 3 to vary ingestion rate for benchmarking.

**Speed Layer — Lambda + DynamoDB** *(Vishwas)*
Lambda is triggered directly by the Kinesis event source mapping (batch size 100,
starting position LATEST). Each invocation buckets records into 1-minute windows per
endpoint and atomically increments `error_count`/`total_count` in DynamoDB via
`UpdateItem ADD`. The "5-minute sliding window" view is computed at query time by
summing the last 5 one-minute buckets — a tumbling-bucket-under-the-hood design that
behaves as a sliding window to any caller (see comments in `04_lambda_speed_layer.py`
for the full reasoning). DynamoDB TTL expires buckets after 1 hour.

**Batch Layer — EMR + PySpark** *(Sonal)*
Processes the full accumulated log history (or full replayed dataset) to compute,
per endpoint: historical average error rate and average response time. Output is
written to S3 in Parquet format, queryable via Athena. This is the "accurate,
comprehensive view over full history" side of the Lambda architecture — correctness
over freshness.

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
