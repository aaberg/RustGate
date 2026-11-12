# syntax=docker/dockerfile:1
ARG RUST_VERSION=1
FROM docker.io/library/rust:${RUST_VERSION}-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --locked --release --bins

FROM docker.io/library/debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 rustgate \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin rustgate
ARG VERSION=dev
ARG REVISION=unknown
ARG SOURCE=unknown
LABEL org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.source="${SOURCE}"
STOPSIGNAL SIGTERM

FROM runtime AS mock-upstream
COPY --from=builder /build/target/release/mock-upstream /usr/local/bin/mock-upstream
USER 10001:10001
ENV RUSTGATE_MOCK_LISTEN_ADDR=127.0.0.1:9090
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl --fail --silent --show-error http://127.0.0.1:9090/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/mock-upstream"]

# Keep the production image as the default target; it never contains the mock.
FROM runtime AS rustgate
RUN install -d -o 10001 -g 10001 -m 0700 /var/lib/rustgate \
    && install -d -m 0755 /etc/rustgate/tls
COPY --from=builder /build/target/release/rustgate /usr/local/bin/rustgate
USER 10001:10001
WORKDIR /var/lib/rustgate
ENV RUSTGATE_CONFIG=/etc/rustgate/rustgate.toml \
    RUSTGATE_LISTEN_ADDR=0.0.0.0:8443 \
    RUSTGATE_DATABASE_PATH=/var/lib/rustgate/rustgate.sqlite3 \
    RUSTGATE_TLS_CERT_PATH=/etc/rustgate/tls/fullchain.pem \
    RUSTGATE_TLS_KEY_PATH=/etc/rustgate/tls/privkey.pem \
    RUSTGATE_HEALTHCHECK_URL=https://localhost:8443/healthz \
    RUSTGATE_HEALTHCHECK_CA_PATH=/etc/rustgate/tls/ca.pem
EXPOSE 8443
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl --fail --silent --show-error --connect-to ::127.0.0.1:8443 --cacert "$RUSTGATE_HEALTHCHECK_CA_PATH" "$RUSTGATE_HEALTHCHECK_URL" || exit 1
ENTRYPOINT ["/usr/local/bin/rustgate"]
