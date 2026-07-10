#!/bin/bash
# Deploy script for speed layer Lambda function
# Run this from an environment with AWS CLI configured (e.g. EC2 Instance Connect)

zip speed_layer.zip lambda_function.py

aws lambda create-function \
  --function-name speed-layer-error-counter \
  --runtime python3.12 \
  --role $LAMBDA_ROLE_ARN \
  --handler lambda_function.lambda_handler \
  --zip-file fileb://speed_layer.zip \
  --timeout 30

aws lambda create-event-source-mapping \
  --function-name speed-layer-error-counter \
  --event-source-arn $KINESIS_STREAM_ARN \
  --batch-size 100 \
  --starting-position LATEST