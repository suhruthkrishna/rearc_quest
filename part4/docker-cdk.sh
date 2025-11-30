#!/bin/bash
# Bash script to run CDK commands in Docker
# Usage: ./docker-cdk.sh <cdk-command>
# Example: ./docker-cdk.sh synth
# Example: ./docker-cdk.sh deploy

# Check if Docker is running
if ! docker ps > /dev/null 2>&1; then
    echo "Error: Docker is not running. Please start Docker Desktop first."
    exit 1
fi

# Build the image if it doesn't exist or if Dockerfile changed
echo "Building/updating CDK Docker image..."
docker compose build --quiet

# Run the CDK command (default to --version if no args)
CMD="${@:---version}"
echo "Running: cdk $CMD"
docker compose run --rm cdk cdk $CMD

