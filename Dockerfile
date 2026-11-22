# Shopping Sessions: the native Rust pipeline in native/.
# The Python/Polars pipeline in pipeline/ is kept as the reference and fallback
# (commit 82a5c68 builds and runs it).
# The scorers are linux/amd64 (Intel Xeon) and build this file natively, so no
# FROM line pins a platform.

FROM rust:1.96-bookworm AS build
WORKDIR /src
# Fetch dependencies in their own layer, so code changes don't refetch them.
COPY native/Cargo.toml native/Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo fetch --locked
COPY native/src ./src
# Two builds: the default x86-64 CPU, and x86-64-v3 (AVX2, BMI1/2, POPCNT, LZCNT).
# The v3 build names its --target, so RUSTFLAGS apply only to our binary, not to
# build scripts (those run on the build machine, which may lack AVX2).
RUN cargo build --release --locked --offline && cp target/release/shopping /shopping-base && cp target/release/launch /launch
RUN if [ "$(uname -m)" = x86_64 ]; then \
      export RUSTFLAGS="-C target-cpu=x86-64-v3" T=x86_64-unknown-linux-gnu; \
      cargo build --release --locked --offline --bin shopping --target $T --target-dir /t3 && cp /t3/$T/release/shopping /shopping-v3; \
    else cp /shopping-base /shopping-v3; fi

# At run time the root filesystem is read-only, the user is 1000:1000, there is
# no network, /data is read-only, and the program writes only to /output.
# /app/shopping is the launcher: it execs shopping-v3 if the CPU has every
# x86-64-v3 feature, else shopping-base.
FROM debian:bookworm-slim
COPY --from=build /launch /app/shopping
COPY --from=build /shopping-base /shopping-v3 /app/
CMD ["/app/shopping"]
