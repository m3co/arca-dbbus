#!/bin/sh

docker build -t arca-dbbus .

docker run -it --rm arca-dbbus