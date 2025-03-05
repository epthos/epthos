#!/bin/bash -e

HOME=$(realpath $(dirname $0)/../..)

echo "Building from $HOME"
cd "${HOME}"

IMAGE_TAG=us-east1-docker.pkg.dev/storied-sound-423401-k4/broker-repository/broker
docker build . -t ${IMAGE_TAG} -f piston/docker/Dockerfile --platform linux/x86_64
docker push ${IMAGE_TAG}
