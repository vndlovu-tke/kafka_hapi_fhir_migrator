#!/bin/bash

docker image build -t hapi-migration:latest1 . && docker stack deploy -c docker/docker-compose.yml migration
