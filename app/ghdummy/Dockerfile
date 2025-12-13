# Build Stage
FROM maven:3.9.12-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn de.qaware.maven:go-offline-maven-plugin:resolve-dependencies
COPY src ./src
RUN mvn clean package -DskipTests

# Package Stage
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /app/target/ghdummy.jar ./ghdummy.jar
EXPOSE 8889
ENTRYPOINT [ "java", "-cp", "ghdummy.jar", "com.rolandb.DummyServer" ]
