FROM postgres:10

RUN apt-get update
RUN apt-get -y install postgresql-10-pgtap

ENV POSTGRES_PASSWORD="test"
ENV POSTGRES_USER="test"
ENV POSTGRES_DB="test-search"

COPY plpgsql-test/db-Search.sql /docker-entrypoint-initdb.d
