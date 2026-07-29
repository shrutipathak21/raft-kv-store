# ---- Build stage ----
# Compiles the project with a full JDK. No Maven/Gradle needed — this project
# is dependency-free, so a straight javac build is all that's required.
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /build
COPY src ./src
RUN find src -name "*.java" > sources.txt && \
    javac -d out -encoding UTF-8 @sources.txt

# ---- Runtime stage ----
# Ships only the compiled classes + a JRE (no compiler, no source) for a
# smaller final image.
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app
COPY --from=build /build/out ./out
COPY config ./config

# Node ID and cluster config path are supplied by `command:` per-service in
# docker-compose.yml (e.g. ["1", "config/cluster-docker.conf"]), so the same
# image is reused unmodified for all 5 nodes.
ENTRYPOINT ["java", "-cp", "out", "com.raftkv.Server"]
