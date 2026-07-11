"""
Speed Layer - AWS Lambda function
Triggered by Kinesis Data Stream records.

Reads server log records, counts 4xx/5xx errors per endpoint
within the current 5-minute window, and writes/updates the
count in DynamoDB.

Field names confirmed against actual Kinesis records on
10 Jul 2026 via `aws kinesis get-records`:
client_ip, timestamp, request_type, endpoint, status_code,
bytes_sent, referrer, user_agent, response_time, ingested_at,
ingested_epoch.
"""

import base64
import json
import time
import boto3
from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('endpoint-error-counts')

WINDOW_SIZE_SECONDS = 300  # 5-minute sliding window
TTL_SECONDS = 3600         # keep records for 1 hour, then auto-expire


def get_window_start(timestamp_epoch):
    """Round down to the nearest 5-minute boundary."""
    return int(timestamp_epoch // WINDOW_SIZE_SECONDS) * WINDOW_SIZE_SECONDS


def lambda_handler(event, context):
    processed = 0
    errors = 0

    for record in event['Records']:
        try:
            # Kinesis records arrive base64-encoded
            payload = base64.b64decode(record['kinesis']['data'])
            log_entry = json.loads(payload)

            # Confirmed field names from actual Kinesis record (checked 10 Jul 2026)
            status_code = int(log_entry.get('status_code', 0))
            endpoint = log_entry.get('endpoint', 'unknown')

            # Producer already supplies a ready-to-use Unix timestamp -
            # no need to parse the Apache-style 'timestamp' field at all
            event_time = log_entry.get('ingested_epoch', time.time())

            # Only count 4xx and 5xx as errors
            if status_code >= 400:
                window_start = get_window_start(event_time)
                expiry_time = int(time.time()) + TTL_SECONDS

                update_error_count(endpoint, window_start, expiry_time)
                errors += 1

            processed += 1

        except Exception as e:
            print(f"Failed to process record: {e}")
            continue

    print(f"Processed {processed} records, {errors} were errors")
    return {
        'statusCode': 200,
        'processed': processed,
        'errors_counted': errors
    }


def update_error_count(endpoint, window_start, expiry_time):
    """
    Atomically increment the error count for this endpoint/window.
    Using update_item with ADD avoids read-then-write race conditions
    when multiple Lambda invocations hit the same window concurrently.
    """
    table.update_item(
        Key={
            'endpoint': endpoint,
            'window_start': window_start
        },
        UpdateExpression='ADD error_count :inc SET expiry_time = :exp',
        ExpressionAttributeValues={
            ':inc': 1,
            ':exp': expiry_time
        }
    )