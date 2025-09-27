#!/bin/bash

IMAGE_NAME=analytics
docker rmi $(docker images -f dangling=true -q) 2> /dev/null

docker build -t $IMAGE_NAME -f Dockerfile .
ARGS="$*"
docker run -v "$(pwd)/duckdb:/app/duckdb" $IMAGE_NAME sh -c "python3 /app/main.py \"$ARGS\""
