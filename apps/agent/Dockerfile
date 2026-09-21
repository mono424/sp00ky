# The agent image: ONE static binary at /spky-agent, nothing else.
#
# It is never run as a service. The docker pool provider (and the VM bootstrap)
# runs `/spky-agent --install <dest>` once to copy the binary out, then mounts it
# into the backend's own container as the entrypoint. Static musl, because it has
# to run inside whatever image the backend happens to be built on.
#
# Build from the repo root:
#   docker build -f apps/agent/Dockerfile -t mono424/spooky-agent:dev .
FROM rust:1.93-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
# Only what the agent needs: the workspace manifest is replaced by a two-member
# one so the rest of the (large) workspace is never resolved or fetched.
COPY packages/pool-protocol packages/pool-protocol
COPY apps/agent apps/agent
RUN printf '[workspace]\nresolver = "2"\nmembers = ["apps/agent", "packages/pool-protocol"]\n' > Cargo.toml \
 && cargo build --release -p spky-agent \
 && strip target/release/spky-agent

FROM scratch
COPY --from=build /src/target/release/spky-agent /spky-agent
ENTRYPOINT ["/spky-agent"]
