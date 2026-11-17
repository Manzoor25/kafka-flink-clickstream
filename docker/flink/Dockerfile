# Stage 1: Build Flink Java DataStream & SQL runner JAR
FROM maven:3.9.9-eclipse-temurin-17 AS builder
WORKDIR /build
COPY flink-jobs/pom.xml ./pom.xml
RUN mvn -B -q dependency:go-offline
COPY flink-jobs/src ./src
COPY schemas /schemas
RUN mvn -B -q package -DskipTests

# Stage 2: Apache Flink 2.2.1 runtime with Kafka 5.0.0 & Avro Confluent Registry connectors
FROM flink:2.2.1-scala_2.12-java17

ARG KAFKA_CONNECTOR_VERSION=5.0.0-2.2
ARG FLINK_VERSION=2.2.1

RUN curl -fsSL -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar" && \
    curl -fsSL -o /opt/flink/lib/flink-sql-avro-confluent-registry-${FLINK_VERSION}.jar \
      "https://repo1.maven.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/${FLINK_VERSION}/flink-sql-avro-confluent-registry-${FLINK_VERSION}.jar" && \
    mkdir -p /opt/flink/usrlib /opt/flink/sql /opt/flink/schemas && \
    chown -R flink:flink /opt/flink/lib /opt/flink/usrlib /opt/flink/sql /opt/flink/schemas

COPY --chown=flink:flink docker/flink/config.yaml /opt/flink/conf/config.yaml
COPY --chown=flink:flink --from=builder /build/target/clickstream-flink-jobs-1.0.0.jar /opt/flink/usrlib/clickstream-flink-jobs.jar
