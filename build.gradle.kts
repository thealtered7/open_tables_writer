plugins {
    java
    application
}

application {
    mainClass.set("com.thealtered7.OpenTablesWriter")
}

group = "com.thealtered7"
version = "0.1.0-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

tasks.withType<JavaCompile>().configureEach {
    options.release.set(21)
}

repositories {
    mavenCentral()
    maven {
        url = uri("https://packages.confluent.io/maven/")
    }
}

val sparkVersion = "3.5.0"
val deltaVersion = "3.1.0"
val icebergVersion = "1.5.0"
val kafkaVersion = "3.9.0"
val jacksonVersion = "2.18.2"
val confluentVersion = "7.5.0"

configurations.all {
    exclude(group = "org.slf4j", module = "slf4j-log4j12")
    // everit-json-schema (via Confluent JSON Schema serializer) pulls scala-library 2.13;
    // Spark 3.5 is Scala 2.12 and fails to compile if the classpath is upgraded.
    resolutionStrategy {
        force("org.scala-lang:scala-library:2.12.18")
    }
}

dependencies {
    implementation(platform("io.micrometer:micrometer-bom:1.14.4"))
    implementation(platform("io.micrometer:micrometer-tracing-bom:1.4.4"))
    implementation(platform("io.opentelemetry:opentelemetry-bom:1.49.0"))

    implementation("io.micrometer:micrometer-observation")
    implementation("io.micrometer:micrometer-core")
    implementation("io.micrometer:micrometer-tracing")
    implementation("io.micrometer:micrometer-tracing-bridge-otel")
    implementation("io.micrometer:micrometer-registry-otlp")
    implementation("io.opentelemetry:opentelemetry-exporter-otlp")
    implementation("io.opentelemetry:opentelemetry-sdk")

    implementation("org.apache.spark:spark-core_2.12:$sparkVersion")
    implementation("org.apache.spark:spark-sql_2.12:$sparkVersion")
    implementation("org.apache.spark:spark-avro_2.12:$sparkVersion")
    implementation("io.delta:delta-spark_2.12:$deltaVersion")
    implementation("org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:$icebergVersion")

    implementation("org.slf4j:slf4j-api:2.0.17")
    implementation("com.fasterxml.jackson.core:jackson-databind:$jacksonVersion")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:$jacksonVersion")
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")
    // Confluent 7.5 pulls mbknor _2.13; Spark needs Scala 2.12 — swap the artifact.
    implementation("io.confluent:kafka-json-schema-serializer:$confluentVersion") {
        exclude(group = "com.kjetland", module = "mbknor-jackson-jsonschema_2.13")
    }
    implementation("com.kjetland:mbknor-jackson-jsonschema_2.12:1.0.39")

    runtimeOnly("ch.qos.logback:logback-classic:1.5.18")
    runtimeOnly("net.logstash.logback:logstash-logback-encoder:8.1")

    testImplementation("org.junit.jupiter:junit-jupiter:5.11.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    environment("HADOOP_USER_NAME", "testuser")
    jvmArgs(
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/java.io=ALL-UNNAMED",
        "--add-opens=java.base/java.net=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/java.util=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
        "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
        "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
    )
}
