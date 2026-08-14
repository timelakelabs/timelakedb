#!/bin/bash
# LocalStack ready-hook for the C2 cluster rig (ARCHITECTURE §12.6): one
# bucket that every node shares. No KMS here on purpose — encryption is
# proven by the C0 rig; this one is about the role split, and fewer moving
# parts means a faster, clearer failure when something is wrong.
set -euo pipefail

awslocal s3api create-bucket --bucket timelake-cluster

echo "timelake cluster init: bucket timelake-cluster ready"
