"""
Server Pulse - Kinesis Producer (for .log files — Apache Combined Log Format)
-------------------------------------------------------------------------------
Reads a raw Apache server log file (NOT a CSV) — one log line per row, like:

    127.0.0.1 - - [10/Oct/2000:13:55:36 -0700] "GET /page.html HTTP/1.1" 200 2326 "http://ref.com" "Mozilla/5.0"

Parses each line with a regex into our canonical JSON schema, and streams it
into AWS Kinesis Data Streams at a configurable rate.

Usage (run in CloudShell, after uploading this file + your .log file):
    pip install boto3 --user
    python3 02_producer.py --file server_logs.log --stream server-pulse-stream --rate 10

If your log lines look different from the format above (e.g. no referrer/user-agent,
or a different date format), paste 2-3 real lines to whoever wrote this and the
LOG_PATTERN regex below can be adjusted — don't guess, check first.
"""
import argparse
import json
import random
import re
import sys
import time
from datetime import datetime, timezone

import boto3

# Matches the zanbil.ir e-commerce access log format (real dataset, 1 GB):
# IP - - [timestamp] "METHOD /path HTTP/version" status bytes "referrer" "user-agent" "extra_field"

LOG_PATTERN = re.compile(
    r'(?P<client_ip>\S+) \S+ \S+ \[(?P<timestamp>[^\]]+)\] '
    r'"(?P<request_type>\S+) (?P<endpoint>\S+) \S+" '
    r'(?P<status_code>\d{3}) (?P<bytes_sent>\S+)'
    r'(?: "(?P<referrer>[^"]*)")?'
    r'(?: "(?P<user_agent>[^"]*)")?'
    r'(?: "(?P<extra_field>[^"]*)")?'
)


def parse_args():
    p = argparse.ArgumentParser(description="Replay a .log file into Kinesis at a controlled rate")
    p.add_argument("--file", required=True, help="Path to the .log file")
    p.add_argument("--stream", default="server-pulse-stream", help="Kinesis stream name")
    p.add_argument("--region", default="us-east-1", help="AWS region")
    p.add_argument("--rate", type=float, default=10.0, help="Target records per second")
    p.add_argument("--loop", action="store_true", help="Loop over the file forever")
    p.add_argument("--jitter", type=float, default=0.3, help="Randomness in inter-record delay (0-1)")
    p.add_argument("--max-records", type=int, default=None, help="Stop after N records (for quick tests)")
    return p.parse_args()


def parse_log_line(line):
    """Parse one raw Apache log line into our canonical JSON schema. Returns None if it doesn't match."""
    match = LOG_PATTERN.match(line.strip())
    if not match:
        return None

    record = match.groupdict()

    # Type coercion — best-effort, never crashes on a weird line
    try:
        record["status_code"] = int(record["status_code"])
    except (TypeError, ValueError):
        pass
    try:
        record["bytes_sent"] = int(record["bytes_sent"])
    except (TypeError, ValueError):
        record["bytes_sent"] = 0  # Apache logs use "-" for zero bytes

    # Stamp with the actual ingestion time — the speed layer windows on THIS,
    # not the log's original (possibly years-old) timestamp. This is what
    # makes replayed data behave like a live stream downstream.
    record["ingested_at"] = datetime.now(timezone.utc).isoformat()
    record["ingested_epoch"] = int(datetime.now(timezone.utc).timestamp())
    return record


def stream_records(filepath, stream_name, region, rate, loop, jitter, max_records):
    kinesis = boto3.client("kinesis", region_name=region)
    delay = 1.0 / rate if rate > 0 else 0
    sent = 0
    unparsed = 0

    while True:
        with open(filepath, encoding="utf-8", errors="replace") as f:
            for line in f:
                if not line.strip():
                    continue
                if max_records and sent >= max_records:
                    print(f"[producer] reached --max-records={max_records}, stopping.", file=sys.stderr)
                    return

                record = parse_log_line(line)
                if record is None:
                    unparsed += 1
                    if unparsed <= 5:
                        print(f"[producer] could not parse line: {line.strip()[:120]}", file=sys.stderr)
                    continue

                partition_key = str(record.get("endpoint") or record.get("client_ip") or "default")

                # Kinesis partition keys must be <= 256 chars — this dataset has
                # some very long URLs (encoded query strings, long slugs) that
                # exceed this, so truncate defensively.
                partition_key = partition_key[:256]
                try:
                    kinesis.put_record(
                        StreamName=stream_name,
                        Data=json.dumps(record).encode("utf-8"),
                        PartitionKey=partition_key,
                    )
                    sent += 1
                    if sent % 50 == 0:
                        print(f"[producer] sent {sent} records... ({unparsed} unparsed so far)", file=sys.stderr)
                except Exception as e:
                    print(f"[producer] put_record failed: {e}", file=sys.stderr)

                sleep_time = delay * (1 + random.uniform(-jitter, jitter)) if delay else 0
                if sleep_time > 0:
                    time.sleep(max(0, sleep_time))
        if not loop:
            break

    print(f"[producer] done. sent={sent} unparsed={unparsed}", file=sys.stderr)
    if unparsed > 0:
        print(f"[producer] WARNING: {unparsed} lines didn't match LOG_PATTERN — "
              f"check the regex against your actual log format.", file=sys.stderr)


if __name__ == "__main__":
    args = parse_args()
    stream_records(
        filepath=args.file,
        stream_name=args.stream,
        region=args.region,
        rate=args.rate,
        loop=args.loop,
        jitter=args.jitter,
        max_records=args.max_records,
    )