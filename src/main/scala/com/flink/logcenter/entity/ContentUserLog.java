package com.flink.logcenter.entity;

public class ContentUserLog {
    private String contentId;
    private String userId;

    public ContentUserLog() {
    }

    public ContentUserLog(String contentId, String userId) {
        this.contentId = contentId;
        this.userId = userId;
    }

    public String getContentId() {
        return contentId;
    }

    public ContentUserLog setContentId(String contentId) {
        this.contentId = contentId;
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public ContentUserLog setUserId(String userId) {
        this.userId = userId;
        return this;
    }
}
