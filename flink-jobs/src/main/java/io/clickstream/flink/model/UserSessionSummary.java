package io.clickstream.flink.model;

import java.io.Serializable;

/**
 * Closed/updated user session summary emitted by SessionAnalyticsJob to analytics.sessions.v1.
 */
public class UserSessionSummary implements Serializable {
    private static final long serialVersionUID = 1L;

    private String sessionId;
    private String userId;
    private long sessionStartMs;
    private long sessionEndMs;
    private double durationSec;
    private long eventCount;
    private long distinctProducts;
    private long viewCount;
    private long cartCount;
    private long checkoutCount;
    private long purchaseCount;
    private boolean converted;
    private String primaryDevice;
    private String jobVersion;

    public UserSessionSummary() {
    }

    public UserSessionSummary(
            String sessionId,
            String userId,
            long sessionStartMs,
            long sessionEndMs,
            double durationSec,
            long eventCount,
            long distinctProducts,
            long viewCount,
            long cartCount,
            long checkoutCount,
            long purchaseCount,
            boolean converted,
            String primaryDevice,
            String jobVersion) {
        this.sessionId = sessionId;
        this.userId = userId;
        this.sessionStartMs = sessionStartMs;
        this.sessionEndMs = sessionEndMs;
        this.durationSec = durationSec;
        this.eventCount = eventCount;
        this.distinctProducts = distinctProducts;
        this.viewCount = viewCount;
        this.cartCount = cartCount;
        this.checkoutCount = checkoutCount;
        this.purchaseCount = purchaseCount;
        this.converted = converted;
        this.primaryDevice = primaryDevice;
        this.jobVersion = jobVersion;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getSessionStartMs() {
        return sessionStartMs;
    }

    public void setSessionStartMs(long sessionStartMs) {
        this.sessionStartMs = sessionStartMs;
    }

    public long getSessionEndMs() {
        return sessionEndMs;
    }

    public void setSessionEndMs(long sessionEndMs) {
        this.sessionEndMs = sessionEndMs;
    }

    public double getDurationSec() {
        return durationSec;
    }

    public void setDurationSec(double durationSec) {
        this.durationSec = durationSec;
    }

    public long getEventCount() {
        return eventCount;
    }

    public void setEventCount(long eventCount) {
        this.eventCount = eventCount;
    }

    public long getDistinctProducts() {
        return distinctProducts;
    }

    public void setDistinctProducts(long distinctProducts) {
        this.distinctProducts = distinctProducts;
    }

    public long getViewCount() {
        return viewCount;
    }

    public void setViewCount(long viewCount) {
        this.viewCount = viewCount;
    }

    public long getCartCount() {
        return cartCount;
    }

    public void setCartCount(long cartCount) {
        this.cartCount = cartCount;
    }

    public long getCheckoutCount() {
        return checkoutCount;
    }

    public void setCheckoutCount(long checkoutCount) {
        this.checkoutCount = checkoutCount;
    }

    public long getPurchaseCount() {
        return purchaseCount;
    }

    public void setPurchaseCount(long purchaseCount) {
        this.purchaseCount = purchaseCount;
    }

    public boolean isConverted() {
        return converted;
    }

    public void setConverted(boolean converted) {
        this.converted = converted;
    }

    public String getPrimaryDevice() {
        return primaryDevice;
    }

    public void setPrimaryDevice(String primaryDevice) {
        this.primaryDevice = primaryDevice;
    }

    public String getJobVersion() {
        return jobVersion;
    }

    public void setJobVersion(String jobVersion) {
        this.jobVersion = jobVersion;
    }
}
