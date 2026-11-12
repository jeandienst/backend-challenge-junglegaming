#!/bin/sh
set -eu

endpoint=http://localstack:4566
dlq_name=wager-transactions-dlq.fifo

aws --endpoint-url="$endpoint" sqs create-queue \
  --queue-name "$dlq_name" \
  --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"false"}'

dlq_url=$(aws --endpoint-url="$endpoint" sqs get-queue-url \
  --queue-name "$dlq_name" --query QueueUrl --output text)
dlq_arn=$(aws --endpoint-url="$endpoint" sqs get-queue-attributes \
  --queue-url "$dlq_url" --attribute-names QueueArn \
  --query 'Attributes.QueueArn' --output text)

transaction_attributes=$(printf \
  '{"FifoQueue":"true","ContentBasedDeduplication":"false","VisibilityTimeout":"30","RedrivePolicy":"{\\"deadLetterTargetArn\\":\\"%s\\",\\"maxReceiveCount\\":\\"5\\"}"}' \
  "$dlq_arn")

aws --endpoint-url="$endpoint" sqs create-queue \
  --queue-name wager-transactions.fifo \
  --attributes "$transaction_attributes"
aws --endpoint-url="$endpoint" sqs create-queue \
  --queue-name wager-events.fifo \
  --attributes '{"FifoQueue":"true","ContentBasedDeduplication":"false"}'
