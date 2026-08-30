package com.streamx.encodingservice.service;

import com.streamx.encodingservice.event.VideoEncodedEvent;
import com.streamx.encodingservice.event.VideoUploadedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.FileSystemUtils;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class EncodingService {

    private static final String VIDEO_ENCODED_TOPIC = "video.encoded";
    // Video quality to encode
    // Format: resolution, bitrate, height
    private static final List<int[]> VIDEO_QUALITIES = Arrays.asList(
            new int[]{1920, 5000, 1080},  // 1080p - 5000k bitrate
            new int[]{1280, 2800, 720},   // 720p - 2800k bitrate
            new int[]{854, 1400, 480}     // 480p - 1400k bitrate
    );
    private final S3Client s3Client;
    private final KafkaTemplate<String, VideoEncodedEvent> kafkaTemplate;
    @Value("${aws.s3.bucket-name}")
    private String bucketName;
    @Value("${ffmpeg.path}")
    private String ffmpegPath;
    @Value("${encoding.base_path}")
    private String basePath;

    /**
     * Main Encoding Pipeline
     * <p>
     * Steps :
     * 1. Download raw video from S3 uploaded by video service
     * 2. Encode video to different qualities using ffmpeg
     * 3. Generate HLS playlist (.m3u8) for each quality
     * 4. Create a master playlist
     * 5. Upload encoded videos back to S3
     * 6. Publish VideoEncodedEvent to Kafka
     *
     * @param videoUploadedEvent Event published by video service when a video is uploaded to S3. Encoding service consumes this event to start the encoding pipeline.
     */
    public void encodeVideo(VideoUploadedEvent videoUploadedEvent) {
        log.info("Starting encoding for video: {}", videoUploadedEvent.getMovieId());

        Path jobDir = Paths.get(basePath).resolve(videoUploadedEvent.getMovieId());

        try {

            Files.createDirectories(jobDir);

            // Step 1: Download raw video from S3
            Path localVideoPath = jobDir.resolve("raw_video.mp4");
            downloadFromS3(videoUploadedEvent.getVideoKey(), localVideoPath);
            log.info("Raw video downloaded to: {}", localVideoPath);

            Path encodedDir = jobDir.resolve("encoded");
            Files.createDirectories(encodedDir);

            // Step 2: Encode to different qualities
            for (int[] qualities : VIDEO_QUALITIES) {
                int width = qualities[0];
                int bitrate = qualities[1];
                int height = qualities[2];

                Path qualityDir = encodedDir.resolve(height + "p");
                Files.createDirectories(qualityDir);

                // Step 3: Generate HLS
                encodeToHLS(localVideoPath, qualityDir, width, height, bitrate);
            }

            // Step 4: Create master playlist
            Path masterPlaylistPath = encodedDir.resolve("master.m3u8");
            generateMasterPlaylist(masterPlaylistPath);
            log.info("Master playlist generated to: {}", masterPlaylistPath);

            // Step 5: Upload encoded videos back to S3
            String encodedPrefix = "encoded/" + videoUploadedEvent.getMovieId() + "/";
            uploadEncodedFilesToS3(encodedDir, encodedPrefix);
            log.info("Encoded videos uploaded to S3 at prefix: {}", encodedPrefix);

            // Step 6: Publish VideoEncodedEvent to kafka
            String masterPlaylistKey = encodedPrefix + "master.m3u8";
            // String hlsUrl = "https://" + bucketName + ".s3.amazonaws.com/" + masterPlaylistKey; To be removed

            VideoEncodedEvent videoEncodedEvent = VideoEncodedEvent.builder()
                    .movieId(videoUploadedEvent.getMovieId())
                    //.hlsUrl(hlsUrl) to be removed
                    .success(true)
                    .build();

            kafkaTemplate.send(VIDEO_ENCODED_TOPIC, videoUploadedEvent.getMovieId(), videoEncodedEvent)
                    .whenComplete((result, ex) -> {

                        if (ex != null)
                            log.error("Failed to publish VideoEncodedEvent", ex);
                        else
                            log.info("VideoEncodedEvent published successfully for video {}", videoUploadedEvent.getMovieId());
                    });
        } catch (Exception e) {

            log.error("Error during encoding pipeline for video {} ", videoUploadedEvent.getMovieId(), e);

            // Publish encodingFailedEvent to VIDEO_ENCODED_TOPIC
            VideoEncodedEvent encodingFailedEvent = VideoEncodedEvent.builder()
                    .movieId(videoUploadedEvent.getMovieId())
                    .success(false)
                    .errorMessage(e.getMessage())
                    .build();

            kafkaTemplate.send(VIDEO_ENCODED_TOPIC, videoUploadedEvent.getMovieId(), encodingFailedEvent)
                    .whenComplete((result, ex) -> {

                        if (ex != null)
                            log.error("Failed to publish EncodingFailedEvent", ex);
                        else
                            log.info("EncodingFailedEvent published successfully");
                    });
        } finally {

            try {
                FileSystemUtils.deleteRecursively(jobDir);
                log.info("Deleted temporary directory: {}", jobDir);
            } catch (IOException e) {
                log.error("Failed to delete temporary directory: {}", jobDir, e);
            }
        }
    }

    private void downloadFromS3(String videoKey, Path localVideoPath) {

        GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                .bucket(bucketName)
                .key(videoKey)
                .build();

        s3Client.getObject(getObjectRequest, localVideoPath);
    }

    /**
     * Encodes a video file to HLS format using FFmpeg.
     * <p>
     * FFmpeg command created:
     * - Multiple .ts segment files
     * - A .m3u8 playlist file for this quality
     *
     * @param localVideoPath the path to the local video file
     * @param qualityDir     the directory to store the encoded quality files
     * @param width          the width of the output video
     * @param height         the height of the output video
     * @param bitrate        the bitrate of the output video
     * @throws IOException          throws IOException
     * @throws InterruptedException throws InterruptedException
     */
    private void encodeToHLS(Path localVideoPath, Path qualityDir,
                             int width, int height, int bitrate) throws IOException, InterruptedException {

        Path playlistPath = qualityDir.resolve("playlist.m3u8");
        Path segmentPattern = qualityDir.resolve("segment_%03d.ts");

        // FFmpeg command for HLS encoding
        List<String> command = Arrays.asList(
                ffmpegPath,
                "-i", localVideoPath.toString(),                       // Input file location, from here it takes the video
                "-vf", "scale=" + width + ":" + height,     // resolution
                "-c:v", "libx264",                          // codec
                "-b:v", bitrate + "k",                      // video bitrate
                "-c:a", "aac",                              // audio codec
                "-b:a", "128k",                             // audio bitrate
                "-hls_time", "10",                          // chunks length
                "-hls_list_size", "0",                      // keep all segments in the playlist
                "-hls_segment_filename", segmentPattern.toString(), // segment naming
                "-f", "hls",                                // output format HLS
                playlistPath.toString()                     // output playlist
        );

        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.redirectErrorStream(true);
        Process process = processBuilder.start();

        try (BufferedReader reader =
                     new BufferedReader(
                             new InputStreamReader(
                                     process.getInputStream()))) {

            String line;

            while ((line = reader.readLine()) != null) {
                log.info("FFmpeg {}p: {}", height, line);
            }
        }

        int exitCode = process.waitFor();
        if (exitCode != 0) {
            throw new RuntimeException("FFmpeg encoding failed for quality " + height + "p. Exit code: " + exitCode);
        }

        log.info("FFmpeg encoding completed for {}p", height);
    }

    /**
     * Generate master HLS playlist that references all quality playlists.
     * This is the file the video player downloads first.
     */
    private void generateMasterPlaylist(Path masterPlaylistPath) throws IOException {
        StringBuilder master = new StringBuilder();
        master.append("#EXTM3U\n");
        master.append("#EXT-X-VERSION:3\n\n");

        // Add each quality to master playlist
        for (int[] quality : VIDEO_QUALITIES) {
            int width = quality[0];
            int bitrate = quality[1];
            int height = quality[2];

            master.append("#EXT-X-STREAM-INF:BANDWIDTH=")
                    .append(bitrate * 1000)
                    .append(",RESOLUTION=").append(width).append("x").append(height)
                    .append(",CODECS=\"avc1.42e01e,mp4a.40.2\"\n");
            master.append(height).append("p/playlist.m3u8\n\n");
        }

        Files.writeString(masterPlaylistPath, master.toString());
    }

    private void uploadEncodedFilesToS3(Path encodedDir, String s3Prefix) throws IOException {

        try (var files = Files.walk(encodedDir)) {

            files.filter(Files::isRegularFile)
                    .forEach(file -> {

                        try {
                            Path relativePath =
                                    encodedDir.relativize(file);

                            String s3Key = s3Prefix + relativePath.toString()
                                    .replace("\\", "/");

                            String contentType = file.getFileName().toString().endsWith(".m3u8")
                                    ? "application/x-mpegURL"
                                    : "video/MP2T";

                            PutObjectRequest request = PutObjectRequest.builder()
                                    .bucket(bucketName)
                                    .key(s3Key)
                                    .contentType(contentType)
                                    .contentLength(Files.size(file))
                                    .build();

                            s3Client.putObject(request, RequestBody.fromFile(file));

                            log.info("Uploaded {} to S3 as {}", file, s3Key);

                        } catch (Exception e) {
                            throw new RuntimeException(
                                    "Failed to upload file: " + file, e
                            );
                        }
                    });
        }
    }
}
