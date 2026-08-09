#!/bin/bash
# LocalStack ready-hook (C0 drill, ARCHITECTURE §12.6): one KMS key
# behind a stable alias, one bucket with default SSE-KMS + Bucket Keys —
# the server-side half of the encryption design. Runs inside the
# localstack container after all services are up.
set -euo pipefail

KEY_ID=$(awslocal kms create-key \
    --description "timelake envelope + SSE key" \
    --query KeyMetadata.KeyId --output text)
awslocal kms create-alias --alias-name alias/timelake --target-key-id "$KEY_ID"

awslocal s3api create-bucket --bucket timelake-data
awslocal s3api put-bucket-encryption --bucket timelake-data \
    --server-side-encryption-configuration '{
      "Rules": [{
        "ApplyServerSideEncryptionByDefault": {
          "SSEAlgorithm": "aws:kms",
          "KMSMasterKeyID": "alias/timelake"
        },
        "BucketKeyEnabled": true
      }]
    }'

# a second bucket for the cargo integration tests, so drill data and
# test data never mix
awslocal s3api create-bucket --bucket timelake-it

echo "timelake init: KMS alias/timelake -> $KEY_ID; buckets timelake-data, timelake-it"
