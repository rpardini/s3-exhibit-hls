package net.pardini.s3exhibit;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

import javax.servlet.http.HttpServletRequest;
import java.time.Duration;
import java.util.List;

@SpringBootApplication
public class S3ExhibitApplication {
    public static void main(String[] args) {
        SpringApplication.run(S3ExhibitApplication.class, args);
    }
}

@org.springframework.stereotype.Controller
@Slf4j
class Controller {

    private final S3Properties s3Properties;

    public Controller(S3Properties s3Properties) {
        this.s3Properties = s3Properties;
        log.info("Controller config: bucket: '{}', region: '{}' ({})", s3Properties.getBucket(), s3Properties.getRegion(), s3Properties.getRegion().metadata().description());
        log.info("Controller config: allowed first paths ({}): '{}'", s3Properties.getAllowedFirstPaths().size(), s3Properties.getAllowedFirstPaths());
    }


    @GetMapping("/{firstDir}/**")
    public ResponseEntity<String> request(@PathVariable String firstDir, HttpServletRequest request) {
        log.info("First dir: {}", firstDir);
        log.info("Path: {}", request.getRequestURI());

        if (!s3Properties.getAllowedFirstPaths().contains(firstDir)) {
            return new ResponseEntity<>("not found", HttpStatus.NOT_FOUND);
        }

        String bucketName = s3Properties.getBucket();
        String keyName = request.getRequestURI();
        S3Presigner presigner = S3Presigner.builder().credentialsProvider(() -> AwsBasicCredentials.create(s3Properties.getAccessKey(), s3Properties.getSecretKey())).region(s3Properties.getRegion()).build();

        PresignedGetObjectRequest presignedGetObjectRequest = presigner.presignGetObject(GetObjectPresignRequest.builder().signatureDuration(Duration.ofMinutes(s3Properties.getDurationMinutes())).getObjectRequest(GetObjectRequest.builder().bucket(bucketName).key(keyName).build()).build());

        log.info("Presigned URL: " + presignedGetObjectRequest.url());

        String url = presignedGetObjectRequest.url().toString();
        presigner.close();

        return ResponseEntity.status(HttpStatus.FOUND).header(HttpHeaders.LOCATION, presignedGetObjectRequest.url().toString()).build();
        //return new ResponseEntity<>(url, HttpStatus.OK);
    }
}

@Configuration
@ConfigurationProperties(prefix = "s3")
@Data
class S3Properties {
    private Region region;
    private String bucket;
    private String accessKey;
    private String secretKey;
    private List<String> allowedFirstPaths;
    private Integer durationMinutes;
}
    

