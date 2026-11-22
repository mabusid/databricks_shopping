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
# Default CPU target. memchr picks the CPU's fastest instructions at run time.
RUN cargo build --release --locked --offline && cp target/release/shopping /shopping

# At run time the root filesystem is read-only, the user is 1000:1000, there is
# no network, /data is read-only, and the program writes only to /output.
FROM debian:bookworm-slim
COPY --from=build /shopping /app/shopping
CMD ["/app/shopping"]
