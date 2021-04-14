package com.flink.logcenter.util;

public class HBaseCell {
    private String cellKey;
    private String family;
    private String qualifier;
    private String value;
    private long timestamp;

    public String getCellKey() {
        return cellKey;
    }

    public HBaseCell setCellKey(String cellKey) {
        this.cellKey = cellKey;
        return this;
    }

    public String getFamily() {
        return family;
    }

    public HBaseCell setFamily(String family) {
        this.family = family;
        return this;
    }

    public String getQualifier() {
        return qualifier;
    }

    public HBaseCell setQualifier(String qualifier) {
        this.qualifier = qualifier;
        return this;
    }

    public String getValue() {
        return value;
    }

    public HBaseCell setValue(String value) {
        this.value = value;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public HBaseCell setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    @Override
    public String toString() {
        return "HBaseCell{" +
                "cellKey='" + cellKey + '\'' +
                ", family='" + family + '\'' +
                ", qualifier='" + qualifier + '\'' +
                ", value='" + value + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
