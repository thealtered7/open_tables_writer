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
SILVER_DATA_DIR ?= $(DATA_DIR)/silver
KAFKA_CONTAINER ?= streaming-kafka
KAFKA_TOPIC ?= cdc-file-write
DELTA_KAFKA_GROUP ?= delta-table-writer
ICEBERG_KAFKA_GROUP ?= iceberg-table-writer
NOTIFICATIONS_KAFKA_TOPIC ?= open-table-write-notifications
TYPE2_KAFKA_GROUP ?= create-type2-dimension
# earliest = replay from start; latest = skip to end
RESET_TO ?= earliest
# Records to rewind (shift-kafka-offsets-back)
OFFSET_SHIFT ?=

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
	run-create-type2-dimension-docker \
	stop-delta-table-writer-docker stop-iceberg-table-writer-docker \
	stop-create-type2-dimension-docker \
	reset-delta-kafka-offsets reset-iceberg-kafka-offsets \
	reset-cdc-file-write-kafka-offsets reset-type2-notifications-kafka-offsets \
	shift-kafka-offsets-back

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

run-create-type2-dimension-docker: build-docker
	@mkdir -p $(DATA_DIR) $(SILVER_DATA_DIR) $(RAW_DATA_DIR)
	docker run -d --rm --name create-type2-dimension \
		--network $(STREAMING_NETWORK) \
		-v $(DATA_DIR):/opt/data \
		-v $(CONFIG_DIR):/config:ro \
		-e TYPE2_DIMENSION_PROPERTIES_PATH=/config/create-type2-dimension.properties \
		-e HADOOP_USER_NAME=testuser \
		-e SPARK_JVM_ARGS="$(SPARK_JVM_ARGS)" \
		-e MANAGEMENT_OTLP_TRACING_ENDPOINT=http://otel-collector:4318/v1/traces \
		-e MANAGEMENT_OTLP_METRICS_EXPORT_URL=http://otel-collector:4318/v1/metrics \
		$(IMAGE_TAG) com.thealtered7.CreateType2Dimension

stop-delta-table-writer-docker:
	docker stop delta-table-writer

stop-iceberg-table-writer-docker:
	docker stop iceberg-table-writer

stop-create-type2-dimension-docker:
	docker stop create-type2-dimension

# Stop the delta writer (if running) and reset its consumer group offsets on cdc-file-write.
# Requires the streaming Kafka container (streaming-kafka) to be running.
# Usage: make reset-delta-kafka-offsets
#        make reset-delta-kafka-offsets RESET_TO=latest
reset-delta-kafka-offsets:
	-docker stop delta-table-writer 2>/dev/null
	docker exec $(KAFKA_CONTAINER) kafka-consumer-groups.sh \
		--bootstrap-server localhost:9092 \
		--group $(DELTA_KAFKA_GROUP) \
		--topic $(KAFKA_TOPIC) \
		--reset-offsets --to-$(RESET_TO) \
		--execute

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

# Reset both delta and iceberg consumer groups on cdc-file-write.
reset-cdc-file-write-kafka-offsets: reset-delta-kafka-offsets reset-iceberg-kafka-offsets

# Shift committed offsets backward by N records so those messages are reprocessed.
# Requires the streaming Kafka container (streaming-kafka) to be running.
# Usage: make shift-kafka-offsets-back KAFKA_GROUP=delta-table-writer OFFSET_SHIFT=10
#        make shift-kafka-offsets-back KAFKA_GROUP=create-type2-dimension KAFKA_TOPIC=open-table-write-notifications OFFSET_SHIFT=5
shift-kafka-offsets-back:
	@test -n "$(KAFKA_GROUP)" || (echo "KAFKA_GROUP is required (e.g. delta-table-writer)"; exit 1)
	@test -n "$(OFFSET_SHIFT)" || (echo "OFFSET_SHIFT is required (e.g. OFFSET_SHIFT=10)"; exit 1)
	@if [ "$(KAFKA_GROUP)" = "$(DELTA_KAFKA_GROUP)" ]; then docker stop delta-table-writer 2>/dev/null || true; \
	elif [ "$(KAFKA_GROUP)" = "$(ICEBERG_KAFKA_GROUP)" ]; then docker stop iceberg-table-writer 2>/dev/null || true; \
	elif [ "$(KAFKA_GROUP)" = "$(TYPE2_KAFKA_GROUP)" ]; then docker stop create-type2-dimension 2>/dev/null || true; fi
	docker exec $(KAFKA_CONTAINER) kafka-consumer-groups.sh \
		--bootstrap-server localhost:9092 \
		--group $(KAFKA_GROUP) \
		--topic $(KAFKA_TOPIC) \
		--reset-offsets --shift-by -$(OFFSET_SHIFT) \
		--execute

# Stop the type2 dimension consumer (if running) and reset its consumer group offsets.
# Requires the streaming Kafka container (streaming-kafka) to be running.
# Usage: make reset-type2-notifications-kafka-offsets
#        make reset-type2-notifications-kafka-offsets RESET_TO=latest
reset-type2-notifications-kafka-offsets:
	-docker stop create-type2-dimension 2>/dev/null
	docker exec $(KAFKA_CONTAINER) kafka-consumer-groups.sh \
		--bootstrap-server localhost:9092 \
		--group $(TYPE2_KAFKA_GROUP) \
		--topic $(NOTIFICATIONS_KAFKA_TOPIC) \
		--reset-offsets --to-$(RESET_TO) \
		--execute