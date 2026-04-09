# Multi-stage build: compile with Maven, run with JRE only
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/target/storage-node-1.0.0.jar app.jar

# Shards land here; docker-compose mounts a named volume at this path
RUN mkdir -p /app/data/shards
ENV STORAGENODE_DATA_DIR=/app/data/shards

EXPOSE 9001
ENTRYPOINT ["java", "-jar", "app.jar"]