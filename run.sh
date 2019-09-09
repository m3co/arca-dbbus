#!/bin/sh

# Build
docker network create arca-dbbus-net
docker container stop arca-dbbus-db arca-dbbus-go

docker build -t arca-dbbus-go -f go.Dockerfile . && \
docker build -t arca-dbbus-db -f db.Dockerfile . && \

# Run
docker run -d --rm --name arca-dbbus-db --net arca-dbbus-net arca-dbbus-db && \
docker run --rm --name arca-dbbus-go --net arca-dbbus-net arca-dbbus-go && \

# Clean-up
docker stop arca-dbbus-db && \
docker network rm arca-dbbus-net
