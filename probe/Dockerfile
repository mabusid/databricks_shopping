# Probe image (DECISIONS.md, "Scorer probes"): copied over the root Dockerfile
# on a probe branch only. Builds the pipeline twice (default CPU, x86-64-v3)
# and the official generator, then runs probe/run.sh.
FROM rust:1.96-bookworm AS build
WORKDIR /src
COPY native/Cargo.toml native/Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo fetch --locked
COPY native/src ./src
RUN cargo build --release --locked --offline && cp target/release/shopping /shopping-base
# The v3 build names its --target, so RUSTFLAGS apply only to our binary, not to
# build scripts (those run on the build machine, which may lack AVX2).
# Its unit tests (scanner fuzz test, timestamps) are built too and run on the scorer's CPU.
RUN if [ "$(uname -m)" = x86_64 ]; then \
      export RUSTFLAGS="-C target-cpu=x86-64-v3" T=x86_64-unknown-linux-gnu; \
      cargo build --release --locked --offline --bin shopping --target $T --target-dir /t3 && cp /t3/$T/release/shopping /shopping-v3 && \
      cargo test --release --locked --offline --bin shopping --target $T --target-dir /t3 --no-run 2>&1 | tee /tmp/t.log && \
      cp "$(sed -n 's/.*(\(\/t3\/[^)]*\)).*/\1/p' /tmp/t.log | head -1)" /shopping-v3-tests; \
    else cp /shopping-base /shopping-v3 && cp /shopping-base /shopping-v3-tests; fi
WORKDIR /gen
COPY probe/generator/ ./
RUN cargo build --release && cp target/release/generate /generate

FROM debian:bookworm-slim
COPY --from=build /shopping-base /shopping-v3 /shopping-v3-tests /generate /app/
COPY probe/run.sh /app/run.sh
CMD ["/bin/bash", "/app/run.sh"]
