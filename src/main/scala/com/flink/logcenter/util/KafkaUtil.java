package com.flink.logcenter.util;

import cn.hutool.setting.Setting;

import java.util.Properties;

public class KafkaUtil {
    public static Properties getProperties(String groupId) {
        Setting setting = new Setting("kafka.setting");

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", setting.getStr("kafka.bootstrap.servers"));
        properties.setProperty("zookeeper.connect", setting.getStr("kafka.zookeeper.connect"));
        properties.setProperty("group.id", groupId);
        return properties;
    }
}
