package com.streamx.encodingservice.service;

import com.streamx.encodingservice.event.VideoUploadedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class VideoUploadedEventConsumer {

    private final EncodingService encodingService;

    /**
     * Listens for video.uploaded kafka topic.
     * Triggers the encoding process when video service uploads a video to S3.
     *
     * Flow:
     * Video Service -> S3 Upload -> Kafka Topic (video.uploaded)
     *                            -> This Consumer
     *                            -> EncodingService.encodeVideo()
     *                            -> Kafka Topic (video.encoded)
     */

    @KafkaListener(topics = "video.uploaded", groupId = "encoding-service-group")
    public void consumeVideoUploadedEvent(VideoUploadedEvent event) {
        log.info("Consuming video.uploaded event for movie: {} file: {}",
                event.getMovieId(), event.getOriginalFileName());

        try{
            encodingService.encodeVideo(event);
        } catch (Exception e) {
            log.error("Error processing video.uploaded event for movie: {}",
                    event.getMovieId(), e);
        }
    }


}
