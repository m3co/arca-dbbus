FROM postgres:10

RUN apt-get update
RUN apt-get -y install postgresql-10-pgtap

ENV POSTGRES_PASSWORD="test"
ENV POSTGRES_USER="test"
ENV POSTGRES_DB="test-view23"

COPY plpgsql-test/db-view23.sql /docker-entrypoint-initdb.d
