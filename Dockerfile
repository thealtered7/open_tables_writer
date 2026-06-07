# --- build ---
FROM eclipse-temurin:21-jdk-jammy AS build
WORKDIR /build

COPY gradlew gradlew.bat settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle/wrapper/ gradle/wrapper/
COPY config/ config/
COPY src/ src/

RUN chmod +x gradlew && ./gradlew installDist --no-daemon

# --- runtime ---
FROM eclipse-temurin:21-jre-jammy
WORKDIR /app

COPY --from=build /build/build/install/open-tables-writer/lib/ /app/lib/
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["com.thealtered7.OpenTablesWriter"]
