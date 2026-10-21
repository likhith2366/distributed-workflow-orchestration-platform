# Console: React + TypeScript, tested and built with Node.
FROM node:22.20.0-bookworm-slim AS console
WORKDIR /console
COPY console/package.json console/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY console/ ./
RUN npm test && npm run build

# Service: Spring Boot jar with the console bundled as static resources.
FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B -q -Dconsole.skip=true dependency:go-offline
COPY src src
COPY --from=console /console/dist console/dist
RUN mvn -B -q -Dconsole.skip=true -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /app
RUN mkdir -p /app/data && chown -R 10001:10001 /app
COPY --from=build /src/target/conductor.jar /app/conductor.jar
USER 10001:10001
ENV DATA_DIR=/app/data BIND_ADDRESS=0.0.0.0
EXPOSE 8080
# CONDUCTOR_ROLE=worker runs a worker; CONDUCTOR_STORE=dynamodb selects the AWS backend.
ENTRYPOINT ["java","-XX:MaxRAMPercentage=70","-XX:+ExitOnOutOfMemoryError","-jar","/app/conductor.jar"]
