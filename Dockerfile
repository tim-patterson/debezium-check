FROM rust:1.55-buster
RUN apt-get update && apt-get install cmake libsasl2-dev -y;
COPY . .
RUN cargo clean
RUN cargo build --release
ENTRYPOINT ["target/release/debezium_check"]