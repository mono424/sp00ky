# syntax=docker/dockerfile:1.7
# Unified Dockerfile for ssp-server and scheduler.
# Build a specific image with: docker buildx build --target ssp ...
# or                          docker buildx build --target scheduler ...

FROM rust:1.93-bookworm AS chef
RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler \
        cmake \
        pkg-config \
        libssl-dev \
        clang \
    && rm -rf /var/lib/apt/lists/*
RUN cargo install --locked cargo-chef
WORKDIR /usr/src/app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS cacher
COPY --from=planner /usr/src/app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo chef cook --release --recipe-path recipe.json \
        -p ssp-server -p scheduler

FROM chef AS builder
COPY . .
COPY --from=cacher /usr/src/app/target target
COPY --from=cacher /usr/local/cargo /usr/local/cargo
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo build --release -p ssp-server -p scheduler

# The admin dashboard, built independently of the Rust stages so buildx runs it
# in parallel and cargo-chef's layer caching is untouched.
FROM node:22-bookworm-slim AS dashboard
RUN corepack enable
WORKDIR /usr/src/app
COPY . .
RUN pnpm install --filter @spooky-sync/dashboard... --frozen-lockfile \
 && pnpm --filter @spooky-sync/dashboard build

FROM debian:bookworm-slim AS runtime-base
RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        libssl3 \
    && rm -rf /var/lib/apt/lists/*

FROM runtime-base AS ssp
RUN mkdir -p /data
COPY --from=builder /usr/src/app/target/release/ssp-server /usr/local/bin/
ENV RUST_LOG=info \
    SP00KY_PERSISTENCE_FILE=/data/sp00ky_state.json
EXPOSE 8667
CMD ["ssp-server"]

FROM runtime-base AS scheduler
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /data/replica
COPY --from=builder /usr/src/app/target/release/scheduler /usr/local/bin/
# Served by the scheduler at /admin on the admin port. Shipped as files rather
# than embedded in the binary so `cargo build -p scheduler` needs no node
# toolchain; the scheduler logs a warning and serves a placeholder if absent.
COPY --from=dashboard /usr/src/app/apps/dashboard/dist /usr/share/spooky/dashboard
ENV RUST_LOG=info \
    SPKY_ADMIN_DIR=/usr/share/spooky/dashboard
# 9667 = ingest/proxy/ssp/metrics, private network only.
# 9668 = the admin dashboard and its API, safe to publish.
# 9669 = the machine pool listener (pool machines dial in), safe to publish.
EXPOSE 9667 9668 9669
# The tag this image is published under. The scheduler names the pool agent
# image by it (mono424/spooky-agent:<same tag>), so the two always speak the
# same pool protocol. Last, so a new version never invalidates a layer above.
ARG SPKY_VERSION=""
ENV SPKY_RELEASE_VERSION=${SPKY_VERSION}
CMD ["scheduler"]
