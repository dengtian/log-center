package com.flink.logcenter.entity;

public class CoeffRelate {
    private String key;
    private double score;

    public CoeffRelate(String key, double score) {
        this.key = key;
        this.score = score;
    }

    public String getKey() {
        return key;
    }

    public CoeffRelate setKey(String key) {
        this.key = key;
        return this;
    }

    public double getScore() {
        return score;
    }

    public CoeffRelate setScore(double score) {
        this.score = score;
        return this;
    }
}
