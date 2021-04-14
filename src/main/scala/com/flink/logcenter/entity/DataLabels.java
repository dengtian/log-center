package com.flink.logcenter.entity;

import java.util.List;

public class DataLabels {
    private String dataId;
    private String type;
    private List<Label> labels;

    public DataLabels() {
    }

    public DataLabels(String dataId, String type, List<Label> labels) {
        this.dataId = dataId;
        this.type = type;
        this.labels = labels;
    }

    public String getDataId() {
        return dataId;
    }

    public DataLabels setDataId(String dataId) {
        this.dataId = dataId;
        return this;
    }

    public String getType() {
        return type;
    }

    public DataLabels setType(String type) {
        this.type = type;
        return this;
    }

    public List<Label> getLabels() {
        return labels;
    }

    public DataLabels setLabels(List<Label> labels) {
        this.labels = labels;
        return this;
    }
}
