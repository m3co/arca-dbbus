#!/bin/sh

# Build
docker network create arca-dbbus-net
docker stop arca-dbbus-go arca-dbbus-db arca-dbbus-db0 arca-dbbus-db-select arca-dbbus-db-search arca-dbbus-db-master arca-dbbus-db-view12 arca-dbbus-db-view23 arca-dbbus-db-view123

docker build -t arca-dbbus-go         -f go.Dockerfile . && \
docker build -t arca-dbbus-db         -f db.Dockerfile . && \
docker build -t arca-dbbus-db0        -f db.Dockerfile . && \
docker build -t arca-dbbus-db-select  -f plpgsql-test/db-Select.Dockerfile . && \
docker build -t arca-dbbus-db-search  -f plpgsql-test/db-Search.Dockerfile . && \
docker build -t arca-dbbus-db-master  -f plpgsql-test/db-master.Dockerfile . && \
docker build -t arca-dbbus-db-view12  -f plpgsql-test/db-view12.Dockerfile . && \
docker build -t arca-dbbus-db-view23  -f plpgsql-test/db-view23.Dockerfile . && \
docker build -t arca-dbbus-db-view123 -f plpgsql-test/db-view123.Dockerfile . && \

# Run
docker run -d --rm --name arca-dbbus-db         --net arca-dbbus-net arca-dbbus-db && \
docker run -d --rm --name arca-dbbus-db0        --net arca-dbbus-net arca-dbbus-db0 && \
docker run -d --rm --name arca-dbbus-db-select  --net arca-dbbus-net arca-dbbus-db-select && \
docker run -d --rm --name arca-dbbus-db-search  --net arca-dbbus-net arca-dbbus-db-search && \
docker run -d --rm --name arca-dbbus-db-master  --net arca-dbbus-net arca-dbbus-db-master && \
docker run -d --rm --name arca-dbbus-db-view12  --net arca-dbbus-net arca-dbbus-db-view12 && \
docker run -d --rm --name arca-dbbus-db-view23  --net arca-dbbus-net arca-dbbus-db-view23 && \
docker run -d --rm --name arca-dbbus-db-view123 --net arca-dbbus-net arca-dbbus-db-view123 && \
docker run    --rm --name arca-dbbus-go         --net arca-dbbus-net arca-dbbus-go go -- test -v -timeout 5s && \

# Clean-up
docker stop arca-dbbus-db arca-dbbus-db0 arca-dbbus-db-select arca-dbbus-db-search arca-dbbus-db-master arca-dbbus-db-view12 arca-dbbus-db-view23 arca-dbbus-db-view123 && \
docker network rm arca-dbbus-net
