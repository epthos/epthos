# syntax=docker/dockerfile:1

ARG RUST_VERSION=1.92.0
ARG APP_NAME=broker

################################################################################
# Create a stage for building the application.

FROM rust:${RUST_VERSION}-bookworm AS build
ARG APP_NAME
WORKDIR /build

# Install host build dependencies.
RUN apt-get update && apt-get install -y protobuf-compiler

# Build the application.
# Leverage a cache mount to /usr/local/cargo/registry/
# for downloaded dependencies, a cache mount to /usr/local/cargo/git/db
# for git repository dependencies, and a cache mount to /app/target/ for
# compiled dependencies which will speed up subsequent builds.
# Leverage a bind mount to the src directory to avoid having to copy the
# source code into the container. Once built, copy the executable to an
# output directory before the cache mounted /app/target is unmounted.
RUN --mount=type=bind,source=.,target=/build/epthos \
    --mount=type=cache,target=/build/target/ \
    --mount=type=cache,target=/usr/local/cargo/git/db \
    --mount=type=cache,target=/usr/local/cargo/registry/ \
cd /build/epthos/rust/                                                     \
&& cargo build --target-dir=/build/target/ --locked --release -p $APP_NAME \
&& cp /build/target/release/$APP_NAME     /bin/                            \
&& cp /build/epthos/docker/$APP_NAME.toml /etc/

################################################################################
# Create a new stage for running the application that contains the minimal
# runtime dependencies for the application. 
FROM debian:bookworm-slim AS final

# Create a non-privileged user that the app will run under.
ARG UID=10001
ARG APP_NAME
ARG HOME=/home/epthos

RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "${HOME}" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser

RUN mkdir "${HOME}" && chown "${UID}" "${HOME}" && chmod 700 "${HOME}"

USER appuser

# Copy the executable & config from the "build" stage.
COPY --from=build /bin/$APP_NAME /bin/
COPY --from=build /etc/$APP_NAME.toml "${HOME}"

# Expose the port that the application listens on.
EXPOSE 50001

# What the container should run when it is started.
CMD ["/bin/broker", "--home", "/home/epthos"]
