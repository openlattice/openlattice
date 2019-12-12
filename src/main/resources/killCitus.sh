#!/bin/sh

docker-compose stop

yes | docker-compose rm

docker-compose ps
