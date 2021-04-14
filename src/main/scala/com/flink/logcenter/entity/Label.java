package com.flink.logcenter.entity;

import java.util.Objects;

public class Label {
    private String label;
    private int weight;

    public Label(String label, int weight) {
        this.label = label;
        this.weight = weight;
    }

    public String getLabel() {
        return label;
    }

    public Label setLabel(String label) {
        this.label = label;
        return this;
    }

    public int getWeight() {
        return weight;
    }

    public Label setWeight(int weight) {
        this.weight = weight;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Label label1 = (Label) o;
        return weight == label1.weight &&
                Objects.equals(label, label1.label);
    }

    @Override
    public int hashCode() {
        return Objects.hash(label, weight);
    }

    @Override
    public String toString() {
        return "Label{" +
                "label='" + label + '\'' +
                ", weight=" + weight +
                '}';
    }
}
