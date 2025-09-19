# Common operations

## Local build

From the top-level directory, run

```shell
docker build -f ./docker/Dockerfile . -t registry.digitalocean.com/epthos/broker:latest
docker push registry.digitalocean.com/epthos/broker:latest
```

```shell
docker run \
    -d -p 443:50001 --log-driver local \
    --name broker --restart always \
    registry.digitalocean.com/epthos/broker:latest
```

> [!note] `--restart always` is needed to ensure the container runs after an update of docker, a reboot, etc.

To update:

```shell
docker image pull registry.digitalocean.com/epthos/broker:latest
docker create -p 443:50001 --log-driver local \
    --name broker_new --restart always \
    registry.digitalocean.com/epthos/broker:latest
docker stop broker && docker start broker_new
# confirm it works
docker container rm broker
docker rename broker_new broker
```

# Initial setup

Install doctl and init with the registry scope.

```shell
doctl registry login 
```
