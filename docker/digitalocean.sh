#!/bin/bash -e

HOME=$(realpath $(dirname $0)/../..)

echo "Building from $HOME"
cd "${HOME}"

IMAGE_TAG=registry.digitalocean.com/piston/broker:latest
docker build . -t ${IMAGE_TAG} -f piston/docker/Dockerfile --platform linux/x86_64
docker push ${IMAGE_TAG}
