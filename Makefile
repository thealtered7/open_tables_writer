GRADLE_VERSION := 8.14.3
GRADLEW := ./gradlew

# Version from build.gradle.kts (same source Gradle uses)
PROJECT_VERSION := $(shell grep -E '^\s*version\s*=' build.gradle.kts | sed -E 's/.*"([^"]+)".*/\1/')
IMAGE_NAME := open_tables_writer
IMAGE_TAG := $(IMAGE_NAME):$(PROJECT_VERSION)
CONFIG_DIR := $(CURDIR)/config
STREAMING_NETWORK ?= streaming_streaming
DATA_DIR ?= /opt/data
RAW_DATA_DIR ?= /opt/data/raw
KAFKA_CONTAINER ?= streaming-kafka
KAFKA_TOPIC ?= cdc-file-write
ICEBERG_KAFKA_GROUP ?= iceberg-table-writer
# earliest = replay from start; latest = skip to end
RESET_TO ?= earliest

# Required for Spark 3.5 on Java 17+ (same flags as Gradle test task)
SPARK_JVM_ARGS := \
	--add-opens=java.base/java.lang=ALL-UNNAMED \
	--add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
	--add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
	--add-opens=java.base/java.io=ALL-UNNAMED \
	--add-opens=java.base/java.net=ALL-UNNAMED \
	--add-opens=java.base/java.nio=ALL-UNNAMED \
	--add-opens=java.base/java.util=ALL-UNNAMED \
	--add-opens=java.base/java.util.concurrent=ALL-UNNAMED \
	--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED \
	--add-opens=java.base/sun.nio.ch=ALL-UNNAMED \
	--add-opens=java.base/sun.util.calendar=ALL-UNNAMED \
	--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED

.PHONY: bootstrap compile test run clean build-docker run-hello-world \
	run-iceberg-table-writer run-delta-table-writer \
	run-delta-table-writer-docker run-iceberg-table-writer-docker \
	stop-delta-table-writer-docker stop-iceberg-table-writer-docker \
	reset-iceberg-kafka-offsets

# One-time: create gradlew + gradle/wrapper/* (requires `gradle` on PATH)
bootstrap:
	gradle wrapper --gradle-version=$(GRADLE_VERSION)
	chmod +x gradlew

# Ensure wrapper exists before compile/test
$(GRADLEW):
	@test -x $(GRADLEW) || (echo "Run: make bootstrap"; exit 1)

compile: $(GRADLEW)
	$(GRADLEW) compileJava

test: $(GRADLEW)
	$(GRADLEW) test

run: $(GRADLEW)
	$(GRADLEW) run

run-iceberg-table-writer: $(GRADLEW)
	$(GRADLEW) installDist -q
	HADOOP_USER_NAME=testuser java $(SPARK_JVM_ARGS) -cp "build/install/open-tables-writer/lib/*" \
		-Dinput.file.path="$${FILE_PATH}" \
		-Ddata.directory.base.path="/opt/data/iceberg" \
		com.thealtered7.IcebergTableWriterOneShot

# "/home/jkeene/src/pgoutput_to_json/pgoutput_to_json_data/geo.public.scalars-2026-05-31_02-51-21.jsonl" \
# "/home/jkeene/src/pgoutput_to_json/pgoutput_to_json_data/geo.public.scalars-2026-06-01_02-14-58.jsonl" \
# "/home/jkeene/src/pgoutput_to_json/pgoutput_to_json_data/geo.public.scalars-2026-06-05_21-58-57.jsonl" \

run-delta-table-writer: $(GRADLEW)
	$(GRADLEW) installDist -q
	HADOOP_USER_NAME=testuser java $(SPARK_JVM_ARGS) -cp "build/install/open-tables-writer/lib/*" \
		-Dinput.file.path=$${FILE_PATH} \
		-Ddata.directory.base.path="/opt/data/deltatable" \
		com.thealtered7.DeltaTableWriterOneShot

clean: $(GRADLEW)
	$(GRADLEW) clean

build-docker:
	docker build -t $(IMAGE_NAME):$(PROJECT_VERSION) .

run-hello-world:
	docker run -it --rm $(IMAGE_TAG)

# pgoutput must write to host $(RAW_DATA_DIR); writer mounts host $(DATA_DIR) so
# notifications referencing /opt/data/raw/... resolve inside the container.
run-delta-table-writer-docker: build-docker
	@mkdir -p $(DATA_DIR) $(RAW_DATA_DIR)
	docker run -d --rm --name delta-table-writer \
		--network $(STREAMING_NETWORK) \
		-v $(DATA_DIR):/opt/data \
		-v $(CONFIG_DIR):/config:ro \
		-e WRITER_COMMON_PROPERTIES_PATH=/config/writer-common.properties \
		-e WRITER_PROPERTIES_PATH=/config/delta-table-writer.properties \
		-e HADOOP_USER_NAME=testuser \
		-e SPARK_JVM_ARGS="$(SPARK_JVM_ARGS)" \
		-e MANAGEMENT_OTLP_TRACING_ENDPOINT=http://otel-collector:4318/v1/traces \
		-e MANAGEMENT_OTLP_METRICS_EXPORT_URL=http://otel-collector:4318/v1/metrics \
		$(IMAGE_TAG) com.thealtered7.DeltaTableWriter

run-iceberg-table-writer-docker: build-docker
	@mkdir -p $(DATA_DIR) $(RAW_DATA_DIR)
	docker run -d --rm --name iceberg-table-writer \
		--network $(STREAMING_NETWORK) \
		-v $(DATA_DIR):/opt/data \
		-v $(CONFIG_DIR):/config:ro \
		-e WRITER_COMMON_PROPERTIES_PATH=/config/writer-common.properties \
		-e WRITER_PROPERTIES_PATH=/config/iceberg-table-writer.properties \
		-e HADOOP_USER_NAME=testuser \
		-e SPARK_JVM_ARGS="$(SPARK_JVM_ARGS)" \
		-e MANAGEMENT_OTLP_TRACING_ENDPOINT=http://otel-collector:4318/v1/traces \
		-e MANAGEMENT_OTLP_METRICS_EXPORT_URL=http://otel-collector:4318/v1/metrics \
		$(IMAGE_TAG) com.thealtered7.IcebergTableWriter

stop-delta-table-writer-docker:
	docker stop delta-table-writer

stop-iceberg-table-writer-docker:
	docker stop iceberg-table-writer

# Stop the iceberg writer (if running) and reset its consumer group offsets.
# Requires the streaming Kafka container (streaming-kafka) to be running.
# Usage: make reset-iceberg-kafka-offsets
#        make reset-iceberg-kafka-offsets RESET_TO=latest
reset-iceberg-kafka-offsets:
	-docker stop iceberg-table-writer 2>/dev/null
	docker exec $(KAFKA_CONTAINER) kafka-consumer-groups.sh \
		--bootstrap-server localhost:9092 \
		--group $(ICEBERG_KAFKA_GROUP) \
		--topic $(KAFKA_TOPIC) \
		--reset-offsets --to-$(RESET_TO) \
		--execute