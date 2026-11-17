package io.clickstream.flink.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * POJO matching schemas/click_event.avsc for Flink DataStream serialization.
 */
public class ClickEvent implements Serializable {
    private static final long serialVersionUID = 1L;

    private String eventId;
    private String userId;
    private String clientSessionId;
    private String eventType;
    private String productId;
    private int quantity;
    private String device;
    private String pageUrl;
    private long eventTsMs;
    private long createdAtMs;
    private boolean syntheticLate;

    public ClickEvent() {
    }

    public ClickEvent(
            String eventId,
            String userId,
            String clientSessionId,
            String eventType,
            String productId,
            int quantity,
            String device,
            String pageUrl,
            long eventTsMs,
            long createdAtMs,
            boolean syntheticLate) {
        this.eventId = eventId;
        this.userId = userId;
        this.clientSessionId = clientSessionId;
        this.eventType = eventType;
        this.productId = productId;
        this.quantity = quantity;
        this.device = device;
        this.pageUrl = pageUrl;
        this.eventTsMs = eventTsMs;
        this.createdAtMs = createdAtMs;
        this.syntheticLate = syntheticLate;
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

    public String getClientSessionId() {
        return clientSessionId;
    }

    public void setClientSessionId(String clientSessionId) {
        this.clientSessionId = clientSessionId;
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

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public String getDevice() {
        return device;
    }

    public void setDevice(String device) {
        this.device = device;
    }

    public String getPageUrl() {
        return pageUrl;
    }

    public void setPageUrl(String pageUrl) {
        this.pageUrl = pageUrl;
    }

    public long getEventTsMs() {
        return eventTsMs;
    }

    public void setEventTsMs(long eventTsMs) {
        this.eventTsMs = eventTsMs;
    }

    public long getCreatedAtMs() {
        return createdAtMs;
    }

    public void setCreatedAtMs(long createdAtMs) {
        this.createdAtMs = createdAtMs;
    }

    public boolean isSyntheticLate() {
        return syntheticLate;
    }

    public void setSyntheticLate(boolean syntheticLate) {
        this.syntheticLate = syntheticLate;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ClickEvent that)) {
            return false;
        }
        return eventTsMs == that.eventTsMs && Objects.equals(eventId, that.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId, eventTsMs);
    }
}
