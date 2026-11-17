package io.clickstream.flink.model;

import java.io.Serializable;

/**
 * Audit record emitted via DataStream side output when event_ts < watermark - allowedLateness.
 */
public class LateClickAudit implements Serializable {
    private static final long serialVersionUID = 1L;

    private String eventId;
    private String userId;
    private String eventType;
    private String productId;
    private long eventTsMs;
    private long watermarkMs;
    private long latenessMs;
    private long detectedAtMs;

    public LateClickAudit() {
    }

    public LateClickAudit(
            String eventId,
            String userId,
            String eventType,
            String productId,
            long eventTsMs,
            long watermarkMs,
            long latenessMs,
            long detectedAtMs) {
        this.eventId = eventId;
        this.userId = userId;
        this.eventType = eventType;
        this.productId = productId;
        this.eventTsMs = eventTsMs;
        this.watermarkMs = watermarkMs;
        this.latenessMs = latenessMs;
        this.detectedAtMs = detectedAtMs;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public long getEventTsMs() {
        return eventTsMs;
    }

    public void setEventTsMs(long eventTsMs) {
        this.eventTsMs = eventTsMs;
    }

    public long getWatermarkMs() {
        return watermarkMs;
    }

    public void setWatermarkMs(long watermarkMs) {
        this.watermarkMs = watermarkMs;
    }

    public long getLatenessMs() {
        return latenessMs;
    }

    public void setLatenessMs(long latenessMs) {
        this.latenessMs = latenessMs;
    }

    public long getDetectedAtMs() {
        return detectedAtMs;
    }

    public void setDetectedAtMs(long detectedAtMs) {
        this.detectedAtMs = detectedAtMs;
    }
}
