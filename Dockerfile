FROM eclipse-temurin:17-jre
WORKDIR /app

# Show Java version...
RUN java -version
RUN ls -la /app/
COPY target/s3exhibit-*.jar /app/s3exhibit.jar

CMD ["java", "-jar", "/app/s3exhibit.jar"]
