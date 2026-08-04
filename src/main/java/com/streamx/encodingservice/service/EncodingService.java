package com.streamx.encodingservice.service;

import com.streamx.encodingservice.config.S3Config;
import com.streamx.encodingservice.event.VideoEncodedEvent;
import com.streamx.encodingservice.event.VideoUploadedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.FileSystemUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class EncodingService {

    private final S3Config s3Config;
    private final KafkaTemplate<String, VideoEncodedEvent> kafkaTemplate;

    @Value("${aws.s3.bucket.name}")
    private String bucketName;

    @Value("${ffmpeg.path}")
    private String ffmpegPath;

    @Value("${encoding.base_path}")
    private String basePath;

    private static final String VIDEO_ENCODED_TOPIC = "video-encoded";

    // Video quality to encode
    // Format: resolution, bitrate, height
    private static final List<int[]> VIDEO_QUALITIES = Arrays.asList(
            new int[]{1920, 5000, 1080},  // 1080p - 5000k bitrate
            new int[]{1280, 2800, 720},   // 720p - 2800k bitrate
            new int[]{854, 1400, 480}     // 480p - 1400k bitrate
    );

    /**
     * Main Encoding Pipeline
     *
     * Steps :
     * 1. Download raw video from S3 uploaded by video service
     * 2. Encode video to different qualities using ffmpeg
     * 3. Generate HLS playlist (.m3u8) for each quality
     * 4. Create a master playlist
     * 5. Upload encoded videos back to S3
     * 6. Publish VideoEncodedEvent to Kafka
     * @param videoUploadedEvent
     * Event published by video service when a video is uploaded to S3. Encoding service consumes this event to start the encoding pipeline.
     */
    public void encodeVideo(VideoUploadedEvent videoUploadedEvent) {
        log.info("Starting encoding for video: {}", videoUploadedEvent.getMovieId());

        Path jobDir = null;
        try {
            jobDir = Paths.get(basePath).resolve(videoUploadedEvent.getMovieId());
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
                log.info("Encoded video to {}p", height);
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
            String hlsUrl = "https://" + bucketName + ".s3.amazonaws.com/" + masterPlaylistKey;

            VideoEncodedEvent videoEncodedEvent = VideoEncodedEvent.builder()
                    .movieId(videoUploadedEvent.getMovieId())
                    .hlsUrl(hlsUrl)
                    .masterPlaylistKey(masterPlaylistKey)
                    .success(true)
                    .errorMessage(null)
                    .build();

            kafkaTemplate.send(VIDEO_ENCODED_TOPIC, videoUploadedEvent.getMovieId(), videoEncodedEvent);
            log.info("VideoEncodedEvent is published for movie: {}", videoUploadedEvent.getMovieId());
        }
        catch (Exception e) {

            log.error("Error during encoding pipeline for video {}: {}", videoUploadedEvent.getMovieId(), e.getMessage());

            // Publish encodingFailedEvent to VIDEO_ENCODED_TOPIC
            VideoEncodedEvent encodingFailedEvent = VideoEncodedEvent.builder()
                    .movieId(videoUploadedEvent.getMovieId())
                    .hlsUrl(null)
                    .masterPlaylistKey(null)
                    .success(false)
                    .errorMessage(e.getMessage())
                    .build();

            kafkaTemplate.send(VIDEO_ENCODED_TOPIC, videoUploadedEvent.getMovieId(), encodingFailedEvent);
        }
        finally {

            try {
                FileSystemUtils.deleteRecursively(jobDir);
                log.info("Deleted temporary directory: {}", jobDir);
            } catch (IOException e) {
                log.error("Failed to delete temporary directory: {}", jobDir, e);
            }
        }
    }
}
