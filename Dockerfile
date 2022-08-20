FROM openjdk:17-alpine

COPY . /
RUN ./gradlew build
ENTRYPOINT ["java", "-jar", "/build/libs/statistics-0.0.1-SNAPSHOT.jar"]
