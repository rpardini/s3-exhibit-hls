package net.pardini.s3exhibit.hls;

import io.lindstrom.m3u8.model.MediaPlaylist;
import io.lindstrom.m3u8.model.MediaSegment;
import io.lindstrom.m3u8.parser.MediaPlaylistParser;
import io.lindstrom.m3u8.parser.PlaylistParserException;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;

import javax.servlet.http.HttpServletRequest;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
public class S3ExhibitHLSApplication {
    public static void main(String[] args) {
        SpringApplication.run(S3ExhibitHLSApplication.class, args);
    }
}

@org.springframework.stereotype.Controller
@Slf4j
class Controller {

    public static final String APPLICATION_VND_APPLE_MPEGURL = "application/vnd.apple.mpegurl";
    public static final MediaType APPLICATION_VND_APPLE_MPEGURL_MEDIATYPE = MediaType.parseMediaType(APPLICATION_VND_APPLE_MPEGURL);
    private final S3Properties s3Properties;

    public Controller(S3Properties s3Properties) {
        this.s3Properties = s3Properties;
        log.info("Controller config: bucket: '{}', region: '{}' ({})", s3Properties.getBucket(), s3Properties.getRegion(), s3Properties.getRegion().metadata().description());
        if (this.s3Properties.getHashSalts() == null) throw new IllegalArgumentException("hashSalts must be set");
        log.info("Current millis {} in an hour: {} in a day: {}", System.currentTimeMillis(), System.currentTimeMillis() + Duration.ofHours(1).toMillis(), System.currentTimeMillis() + Duration.ofDays(1).toMillis());
    }


    @GetMapping("/{ts}/{hash}/**")
    @SneakyThrows
    public ResponseEntity<String> request(@PathVariable long ts, @PathVariable String hash, HttpServletRequest request) {
        String path = request.getRequestURI().substring(("/%s/%s/".formatted(ts, hash)).length()); // What's left after ts and hash
        String basePath = path.substring(0, new URI(path).getPath().lastIndexOf('/')); // Get the base path of the request, eg. the dir name
        log.info("Request for path: '{}', ts: {}, hash: {} (basePath: {})", path, ts, hash, basePath);

        // Make sure TS is in the future, otherwise bail.
        if (ts < System.currentTimeMillis()) {
            log.warn("Request for TS in the past: {} - now: {}", ts, System.currentTimeMillis());
            return ResponseEntity.status(HttpStatus.GONE).build();
        }

        // Calculate the possible hashes over the salts, if none match, bail.
        if (s3Properties.getHashSalts().stream().map(salt -> {
            String correctHash = DigestUtils.md5Hex("%s%s%s".formatted(String.valueOf(ts), path, salt)).toUpperCase();
            log.info("One possible hash: {}", correctHash);
            return correctHash;
        }).noneMatch(possibleHash -> possibleHash.equals(hash))) {
            log.warn("Request for hash that doesn't match: {}", hash);
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).build();
        }

        // Authenticated request; get the object from S3, parse it, rewrite its references, etc.
        try (var s3Client = S3Client.builder().credentialsProvider(() -> AwsBasicCredentials.create(s3Properties.getAccessKey(), s3Properties.getSecretKey())).region(s3Properties.getRegion()).build()) {

            // Get metadata from the object in S3 and the object types in a single request.
            var responseWrapper = s3Client.getObject(GetObjectRequest.builder().bucket(s3Properties.getBucket()).key(path).build());
            var response = responseWrapper.response();
            log.info("S3 Response: {}", response);

            // https://www.iana.org/assignments/media-types/application/vnd.apple.mpegurl
            if (!response.contentType().equals(APPLICATION_VND_APPLE_MPEGURL)) {
                log.warn("Request for non-HLS content: {} at path {}", response.contentType(), path);
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();
            }

            // Prepare the presigner.
            try (S3Presigner presigner = S3Presigner.builder()
                    .credentialsProvider(() -> AwsBasicCredentials.create(s3Properties.getAccessKey(), s3Properties.getSecretKey()))
                    .region(s3Properties.getRegion()).build()) {

                // Actual work, functional style
                String newM3u8 = presignAllSegmentsInPlaylist(basePath, new String(responseWrapper.readAllBytes(), StandardCharsets.UTF_8), presigner, ts);

                return ResponseEntity.ok()
                        .header("X-Transformer", "s3-exhibit-hls")
                        .contentType(APPLICATION_VND_APPLE_MPEGURL_MEDIATYPE)
                        .contentLength(newM3u8.length())
                        .body(newM3u8);
            }

        }

    }

    private String presignAllSegmentsInPlaylist(String basePath, String m3u8OriginalContents, S3Presigner presigner, long ts) throws PlaylistParserException {
        // Parse and update playlist. Library uses Immutable types only, so we've to go around a bit.
        MediaPlaylist originalPlaylist = new MediaPlaylistParser().readPlaylist(m3u8OriginalContents);
        MediaPlaylist playList = MediaPlaylist.builder()
                .from(MediaPlaylist.builder().from(originalPlaylist).mediaSegments(new ArrayList<>()).build()) // original playlist, minus the segments
                .version(originalPlaylist.version().orElse(0) + 1) // increment version
                .mediaSegments(
                        originalPlaylist.mediaSegments().stream().map(
                                originalMediaSegment ->
                                        MediaSegment.builder()
                                                .from(originalMediaSegment)
                                                .uri(
                                                        presigner.presignGetObject(
                                                                GetObjectPresignRequest.builder()
                                                                        .signatureDuration(Duration.ofMillis(ts - System.currentTimeMillis())) // since it requires a duration, calculate it
                                                                        .getObjectRequest(
                                                                                GetObjectRequest.builder()
                                                                                        .bucket(s3Properties.getBucket())
                                                                                        .key("%s/%s".formatted(basePath, originalMediaSegment.uri()))
                                                                                        .build()
                                                                        ).build()
                                                        ).url().toString()
                                                ).build()
                        ).toList()
                )
                .build();

        return new MediaPlaylistParser().writePlaylistAsString(playList);
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

    private List<String> hashSalts;
}
    

