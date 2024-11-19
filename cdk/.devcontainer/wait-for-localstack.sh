#!/bin/bash
set -e

echo "Waiting for LocalStack to be ready..."
timeout=30
counter=0
while ! curl -s http://localhost:4566/_localstack/health >/dev/null; do
    counter=$((counter + 1))
    if [ $counter -gt $timeout ]; then
        echo "Timeout waiting for LocalStack to be ready"
        exit 1
    fi
    echo "Waiting for LocalStack..."
    sleep 1
done
echo "LocalStack is ready!" 