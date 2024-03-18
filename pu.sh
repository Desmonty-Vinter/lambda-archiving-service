#!/bin/bash

ID=$1

docker build -t lambda-archiving-service .
docker tag lambda-archiving-service:latest ${ID}.dkr.ecr.eu-central-1.amazonaws.com/lambda-archiving-service:latest
docker push ${ID}.dkr.ecr.eu-central-1.amazonaws.com/lambda-archiving-service:latest