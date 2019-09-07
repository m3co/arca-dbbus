#!/bin/sh

docker build -t arca-dbbus .

docker run --rm arca-dbbus