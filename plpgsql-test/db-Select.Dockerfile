FROM postgres:10

RUN apt-get update
RUN apt-get -y install postgresql-10-pgtap

ENV POSTGRES_PASSWORD="test"
ENV POSTGRES_USER="test"
ENV POSTGRES_DB="test-ss"

COPY plpgsql-test/db-Select.sql /docker-entrypoint-initdb.d
