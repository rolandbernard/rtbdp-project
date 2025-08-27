# Build Stage
FROM maven:3.9.11-eclipse-temurin-17 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn de.qaware.maven:go-offline-maven-plugin:resolve-dependencies
COPY src ./src
RUN mvn clean package -DskipTests

# Package Stage
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build /app/target/ghdummy.jar ./ghdummy.jar
EXPOSE 8888
ENTRYPOINT [ "java", "-cp", "ghdummy.jar", "com.rolandb.DummyServer" ]
