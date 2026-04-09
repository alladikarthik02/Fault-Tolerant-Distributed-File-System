FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /build
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B -DskipTests package

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /build/target/distributed-file-system-1.0.0.jar app.jar

RUN mkdir -p /app/data/metadata
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]