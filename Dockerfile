FROM eclipse-temurin:17-jre
COPY target/s3exhibit-*.jar /app/s3exhibit.jar
WORKDIR /app

# Show Java version...
RUN java -version
RUN ls -la /app/

CMD ["java", "-jar", "/app/s3exhibit.jar"]
