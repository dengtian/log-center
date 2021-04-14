package com.flink.logcenter.util;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.flink.logcenter.entity.DataLabels;
import com.flink.logcenter.entity.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

public class LogUtil {

    private static Logger logger = LoggerFactory.getLogger(LogUtil.class);


    private static Set<String> typeSet = new HashSet<>();

    static {
        typeSet.add("text_content");
        typeSet.add("video");
        typeSet.add("small_video");
        typeSet.add("qa");
        typeSet.add("fact");
        typeSet.add("circle");
        typeSet.add("task");
        typeSet.add("user_type");
        typeSet.add("product");
        typeSet.add("shop");
        typeSet.add("activity");
        typeSet.add("service");
        typeSet.add("program");
        typeSet.add("ad");
        typeSet.add("column");
        typeSet.add("dept");
    }

    public static void saveLog(Log log) {
        if (log == null || StrUtil.isEmpty(log.getLogType()) || StrUtil.isEmpty(log.getUserId()) || StrUtil.isEmpty(log.getDataId())) {
            return;
        }

        saveUserLog(log.getUserId(), log.getLogType(), log.getDataId());

        DataLabels dataLabel = ContentHBaseUtil.getDataLabel(log.getDataId(), log.getLogType());
        if (dataLabel == null || CollUtil.isEmpty(dataLabel.getLabels())) {
            return;
        }

        if (typeSet.contains(log.getLogType())) {
            log.getLabels().stream().filter(label -> label != null && StrUtil.isNotEmpty(label.getLabel()) && label.getWeight() > 0).forEach(label -> {
                try {
                    HBaseClient.putData(HBaseConst.USER_LABEL, log.getUserId(), log.getLogType(), label.getLabel(), String.valueOf(label.getWeight()));
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("保存用户兴趣日志失败", e);
                }
            });
        }

    }

    public static void saveUserLog(String userId, String type, String dataId) {
        if (StrUtil.isNotEmpty(userId) && StrUtil.isNotEmpty(type) && StrUtil.isNotEmpty(dataId)) {
            if (typeSet.contains(type)) {
                try {
                    HBaseClient.increaseColumn(HBaseConst.USER_LOG, userId, "data", dataId, 1);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("保存用户行为日志错误", e);
                }
            }
        }
    }
}
