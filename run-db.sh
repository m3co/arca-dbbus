#!/bin/sh

# Build
docker network create arca-dbbus-net

docker build -t arca-dbbus-go -f go.Dockerfile . && \
docker run -d --rm --name arca-dbbus-db --net arca-dbbus-net arca-dbbus-db
