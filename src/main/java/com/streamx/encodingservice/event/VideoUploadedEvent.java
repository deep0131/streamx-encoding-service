package com.streamx.encodingservice.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class VideoUploadedEvent {
    private String movieId;
    private String videoKey;
    private String bucketName;
    private String originalFileName;
    private long fileSizeBytes;
}