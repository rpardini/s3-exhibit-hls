FROM eclipse-temurin:17-jre
WORKDIR /app

# Show Java version...
RUN java -version
RUN ls -la /app/
COPY target/s3exhibit-hls-*.jar /app/s3exhibit-hls.jar

CMD ["java", "-jar", "/app/s3exhibit-hls.jar"]
