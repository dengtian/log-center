package com.flink.logcenter.entity;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;

import java.util.List;

public class Content {
    private String contentId;
    private String title;
    private String timestamp;
    private String fileType;
    private List<Label> labels;
    private String authorId;
    private String authorArea;

    public static Content getInstance(String log) {
        if (StrUtil.isNotEmpty(log)) {
            try {
                return JSONUtil.toBean(log, Content.class);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return null;
    }

    public String getTitle() {
        return title;
    }

    public Content setTitle(String title) {
        this.title = title;
        return this;
    }

    public String getContentId() {
        return contentId;
    }

    public Content setContentId(String contentId) {
        this.contentId = contentId;
        return this;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public Content setTimestamp(String timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public String getFileType() {
        return fileType;
    }

    public Content setFileType(String fileType) {
        this.fileType = fileType;
        return this;
    }

    public List<Label> getLabels() {
        return labels;
    }

    public Content setLabels(List<Label> labels) {
        this.labels = labels;
        return this;
    }

    public String getAuthorId() {
        return authorId;
    }

    public Content setAuthorId(String authorId) {
        this.authorId = authorId;
        return this;
    }

    public String getAuthorArea() {
        return authorArea;
    }

    public Content setAuthorArea(String authorArea) {
        this.authorArea = authorArea;
        return this;
    }

    public double getTotal() {
        double res = 0;
        if (CollUtil.isNotEmpty(getLabels())) {
            res = getLabels().stream().filter(e -> StrUtil.isNotEmpty(e.getLabel()) && e.getWeight() > 0).mapToDouble(e -> Math.pow(e.getWeight(), 2)).sum();
        }
        return res;
    }

    @Override
    public String toString() {
        return "Content{" +
                "contentId='" + contentId + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", fileType='" + fileType + '\'' +
                ", labels=" + labels +
                ", authorId='" + authorId + '\'' +
                ", authorArea='" + authorArea + '\'' +
                '}';
    }
}
