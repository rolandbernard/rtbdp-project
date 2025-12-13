# Build Stage
FROM maven:3.9.12-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn de.qaware.maven:go-offline-maven-plugin:resolve-dependencies
COPY src ./src
RUN mvn clean package -DskipTests

# Flink Stage
FROM flink:2.1.1-java21
WORKDIR /app
COPY --from=build /app/target/processor.jar ./processor.jar
COPY submit.sh .

# Submit to Flink Cluster
ENTRYPOINT [ "bash", "submit.sh" ]
