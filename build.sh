set -e;
docker build -t debezium_check .
docker create --name debezium_check debezium_check
mkdir -p release/linux
docker cp debezium_check:/target/release/debezium_check release/linux/debezium_check
docker rm debezium_check