GRADLE_VERSION := 8.14.3
GRADLEW := ./gradlew

# Version from build.gradle.kts (same source Gradle uses)
PROJECT_VERSION := $(shell grep -E '^\s*version\s*=' build.gradle.kts | sed -E 's/.*"([^"]+)".*/\1/')
IMAGE_NAME := open_tables_writer

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

.PHONY: bootstrap compile test run clean build-docker run-iceberg-table-writer

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
		-Dinput.file.path="/home/jkeene/src/pgoutput_to_json/pgoutput_to_json_data/geo.public.scalars-2026-05-31_02-51-21.jsonl" \
		-Ddata.directory.base.path="/opt/data/iceberg" \
		com.thealtered7.IcebergTableWriter

clean: $(GRADLEW)
	$(GRADLEW) clean

build-docker:
	docker build -t $(IMAGE_NAME):$(PROJECT_VERSION) .

run-hello-world:
	docker run -it --rm $(IMAGE_NAME):$(PROJECT_VERSION)