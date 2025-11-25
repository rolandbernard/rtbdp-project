# Build Stage (Java Server)
FROM maven:3.9.11-eclipse-temurin-17 AS build-java
WORKDIR /app
COPY pom.xml .
RUN mvn de.qaware.maven:go-offline-maven-plugin:resolve-dependencies
COPY src ./src
RUN mvn clean package -DskipTests

# Build Stage (Web Application)
FROM node:25-trixie-slim AS build-web
WORKDIR /app
COPY web ./web
WORKDIR /app/web
RUN npm install
RUN npm run build

# Package Stage
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app
COPY --from=build-java /app/target/frontend.jar ./frontend.jar
COPY --from=build-web /app/web/dist ./web/dist
EXPOSE 8888
EXPOSE 8887
ENTRYPOINT [ "java", "-cp", "frontend.jar", "com.rolandb.Frontend" ]
