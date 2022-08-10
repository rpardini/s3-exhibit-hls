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
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.config.annotation.ContentNegotiationConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
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
public class S3ExhibitHLSApplication implements WebMvcConfigurer {

    public static void main(String[] args) {
        SpringApplication.run(S3ExhibitHLSApplication.class, args);
    }

    /**
     * Spring goes out of it's way to avoid RFD's, which add a Content-Disposition header automatically.
     * We gotta register the m3u8 extension to something starting with "audio/" so that it doesn't.
     */
    @Override
    public void configureContentNegotiation(ContentNegotiationConfigurer configurer) {
        configurer.mediaType("m3u8", MediaType.valueOf(Controller.MEDIATYPE_AUDIO_MPEGURL));
    }
}

@org.springframework.stereotype.Controller
@Slf4j
class Controller {

    public static final String MEDIATYPE_APPLICATION_VND_APPLE_MPEGURL = "application/vnd.apple.mpegurl";
    public static final String MEDIATYPE_AUDIO_MPEGURL = "audio/mpegurl";
    public static final String MEDIATYPE_BINARY_OCTET_STREAM = "binary/octet-stream";
    private final S3Properties s3Properties;

    public Controller(S3Properties s3Properties) {
        this.s3Properties = s3Properties;
        log.info("Controller config: bucket: '{}', region: '{}' ({})", s3Properties.getBucket(), s3Properties.getRegion(), s3Properties.getRegion().metadata().description());
        if (this.s3Properties.getHashSalts() == null) throw new IllegalArgumentException("hashSalts must be set");
        log.info("Current millis {} in an hour: {} in a day: {}", System.currentTimeMillis(), System.currentTimeMillis() + Duration.ofHours(1).toMillis(), System.currentTimeMillis() + Duration.ofDays(1).toMillis());
    }


    @RequestMapping(value = "/{ts}/{hash}/**", method = {RequestMethod.GET, RequestMethod.HEAD}, produces = {MEDIATYPE_APPLICATION_VND_APPLE_MPEGURL, MEDIATYPE_AUDIO_MPEGURL, MEDIATYPE_BINARY_OCTET_STREAM})
    @SneakyThrows
    public ResponseEntity<String> request(@PathVariable long ts, @PathVariable String hash, HttpServletRequest request) {
        String path = request.getRequestURI().substring(("/%s/%s/".formatted(ts, hash)).length()); // What's left after ts and hash
        log.info("Request for path: '{}', ts: {}, hash: {}", path, ts, hash);


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
            var s3meta = responseWrapper.response();
            log.info("S3 Response: {}", s3meta);

            // Prepare the presigner.
            try (S3Presigner presigner = S3Presigner.builder()
                    .credentialsProvider(() -> AwsBasicCredentials.create(s3Properties.getAccessKey(), s3Properties.getSecretKey()))
                    .region(s3Properties.getRegion()).build()) {

                if (s3meta.contentType().equals(MEDIATYPE_APPLICATION_VND_APPLE_MPEGURL) || s3meta.contentType().equals(MEDIATYPE_AUDIO_MPEGURL)) {
                    log.info("Working HLS playlist rewriting for content-type: {}", s3meta.contentType());
                    // Rewrite the HLS playlist. Use newM3u8 String, to pass length to the response.
                    String newM3u8 = presignAllSegmentsInPlaylist(
                            path.substring(0, new URI(path).getPath().lastIndexOf('/')),
                            new String(responseWrapper.readAllBytes(), StandardCharsets.UTF_8),
                            presigner,
                            ts);
                    return ResponseEntity.ok()
                            .header("Access-Control-Allow-Origin", "*") // CORS allow
                            .contentType(MediaType.parseMediaType(MEDIATYPE_AUDIO_MPEGURL))
                            .contentLength(newM3u8.length())
                            .body(newM3u8);
                }

                responseWrapper.abort(); // after this, we're not gonna read the body, instead, we'll redirect to the presigned URL

                if (s3meta.contentType().equals(MEDIATYPE_BINARY_OCTET_STREAM)) {

                    log.info("Got non HLS media type: {}", s3meta.contentType());
                    return ResponseEntity
                            .status(HttpStatus.FOUND)
                            .header("Access-Control-Allow-Origin", "*") // CORS allow
                            .header(HttpHeaders.LOCATION,
                                    presigner.presignGetObject(
                                            GetObjectPresignRequest.builder()
                                                    .signatureDuration(Duration.ofMillis(ts - System.currentTimeMillis())) // since it requires a duration, calculate it
                                                    .getObjectRequest(GetObjectRequest.builder().bucket(s3Properties.getBucket()).key(path).build())
                                                    .build()
                                    ).url().toString()
                            ).build();
                }

                log.warn("Request for non-HLS, non-Audio content: {} at path {}", s3meta.contentType(), path);
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).build();

            }

        }

    }

    private String presignAllSegmentsInPlaylist(String basePath, String m3u8OriginalContents, S3Presigner s3presigner, long timestamp) throws PlaylistParserException {
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
                                                        s3presigner.presignGetObject(
                                                                GetObjectPresignRequest.builder()
                                                                        .signatureDuration(Duration.ofMillis(timestamp - System.currentTimeMillis())) // since it requires a duration, calculate it
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
    

