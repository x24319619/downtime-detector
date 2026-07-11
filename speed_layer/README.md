# Speed Layer

AWS Lambda function triggered by Kinesis Data Stream records from
`server-pulse-stream`. Counts 4xx/5xx errors per endpoint within
5-minute sliding windows and writes counts to DynamoDB table
`endpoint-error-counts`.

## Confirmed Kinesis record fields (checked 10 Jul 2026)
client_ip, timestamp, request_type, endpoint, status_code,
bytes_sent, referrer, user_agent, response_time, ingested_at,
ingested_epoch

## Deployment
See deployment commands in `speed_layer/deploy.sh`