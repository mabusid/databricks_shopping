# Probe image (DECISIONS.md, "Scorer probes"): copied over the root Dockerfile
# on a probe branch only. Builds the pipeline twice (default CPU, x86-64-v3)
# and the official generator, then runs probe/run.sh.
FROM rust:1.96-bookworm AS build
WORKDIR /src
COPY native/Cargo.toml native/Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo fetch --locked
COPY native/src ./src
RUN cargo build --release --locked --offline && cp target/release/shopping /shopping-base
RUN if [ "$(uname -m)" = x86_64 ]; then \
      RUSTFLAGS="-C target-cpu=x86-64-v3" cargo build --release --locked --offline --target-dir /t3 && cp /t3/release/shopping /shopping-v3; \
    else cp /shopping-base /shopping-v3; fi
WORKDIR /gen
COPY probe/generator/ ./
RUN cargo build --release && cp target/release/generate /generate

FROM debian:bookworm-slim
COPY --from=build /shopping-base /shopping-v3 /generate /app/
COPY probe/run.sh /app/run.sh
CMD ["/bin/bash", "/app/run.sh"]
