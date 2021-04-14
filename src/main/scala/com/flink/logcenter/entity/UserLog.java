package com.flink.logcenter.entity;

import java.util.List;

public class UserLog {
    private String userId;
    private List<String> dataIds;

    public String getUserId() {
        return userId;
    }

    public UserLog setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public List<String> getDataIds() {
        return dataIds;
    }

    public UserLog setDataIds(List<String> dataIds) {
        this.dataIds = dataIds;
        return this;
    }
}
