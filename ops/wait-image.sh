#!/bin/sh
# Block until a docker image tag exists. Used by the performance cycle to
# wait on a release build that runs in the background.
#   wait-image.sh <repo:tag>
set -e
TAG="$1"
until docker image inspect "$TAG" >/dev/null 2>&1; do sleep 10; done
echo "IMAGE READY $TAG"
