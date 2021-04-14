package com.flink.logcenter.entity;

import java.util.List;

public class Log {
    private String logType;
    private String dataId;
    private String userId;
    private List<Label> labels;

    public String getUserId() {
        return userId;
    }

    public Log setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public String getLogType() {
        return logType;
    }

    public Log setLogType(String logType) {
        this.logType = logType;
        return this;
    }

    public String getDataId() {
        return dataId;
    }

    public Log setDataId(String dataId) {
        this.dataId = dataId;
        return this;
    }

    public List<Label> getLabels() {
        return labels;
    }

    public Log setLabels(List<Label> labels) {
        this.labels = labels;
        return this;
    }
}
