#!/bin/sh
set -e

echo "Building application..."
mkdir -p /app/bin
cd /app
go build -o /app/bin/api /app/src/cmd/api

echo "Starting application..."
/app/bin/api 