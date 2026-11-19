# The relay image: ONE static binary at /sp00ky-relay, nothing else.
#
# Same shape as the agent image (apps/agent/Dockerfile): a static musl build in
# an empty image. The TLS roots are compiled in (webpki-roots), so an https
# database URL works without a certificate store.
#
# Build from the repo root:
#   docker build -f apps/relay/Dockerfile -t mono424/spooky-relay:dev .
FROM rust:1.96-alpine AS build
RUN apk add --no-cache musl-dev
WORKDIR /src
# Only what the relay needs: the workspace manifest is replaced by a one-member
# one so the rest of the (large) workspace is never resolved or fetched. The
# lock file comes along, so the dependency versions are the ones the workspace
# builds and tests with, and cargo drops the entries this one crate does not
# use.
COPY Cargo.lock Cargo.lock
COPY apps/relay apps/relay
RUN printf '[workspace]\nresolver = "2"\nmembers = ["apps/relay"]\n' > Cargo.toml \
 && cargo build --release -p sp00ky-relay \
 && strip target/release/sp00ky-relay

FROM scratch
COPY --from=build /src/target/release/sp00ky-relay /sp00ky-relay
# Any unprivileged user will do: the relay writes nothing and its port is above
# 1024.
USER 65534:65534
ENV RUST_LOG=info
EXPOSE 8668
ENTRYPOINT ["/sp00ky-relay"]
