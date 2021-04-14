package com.flink.logcenter.util;

public class Area {
    //行政编码
    private String code ;

    //名称
    private String name;

    //行政级别 0:省/直辖市 1:地级市 2:县级市
    private int level;

    //上一级的行政区划代码
    private String parentCode;

    private String province;
    private String district;
    private String county;

    public Area() {
    }

    public Area(String code, String name, int level, String parentCode) {
        this.code = code;
        this.name = name;
        this.level = level;
        this.parentCode = parentCode;
    }

    public Area(String code, String name, int level, String parentCode, String province, String district, String county) {
        this.code = code;
        this.name = name;
        this.level = level;
        this.parentCode = parentCode;
        this.province = province;
        this.district = district;
        this.county = county;
    }

    public String getProvince() {
        return province;
    }

    public Area setProvince(String province) {
        this.province = province;
        return this;
    }

    public String getDistrict() {
        return district;
    }

    public Area setDistrict(String district) {
        this.district = district;
        return this;
    }

    public String getCounty() {
        return county;
    }

    public Area setCounty(String county) {
        this.county = county;
        return this;
    }

    public String getCode() {
        return code;
    }

    public Area setCode(String code) {
        this.code = code;
        return this;
    }

    public String getName() {
        return name;
    }

    public Area setName(String name) {
        this.name = name;
        return this;
    }

    public int getLevel() {
        return level;
    }

    public Area setLevel(int level) {
        this.level = level;
        return this;
    }

    public String getParentCode() {
        return parentCode;
    }

    public Area setParentCode(String parentCode) {
        this.parentCode = parentCode;
        return this;
    }
}
