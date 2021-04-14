package com.flink.logcenter.util;

import cn.hutool.core.util.StrUtil;
import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Date;

public class MysqlUti {

    private static Logger logger = LoggerFactory.getLogger(MysqlUti.class);

    public static void saveLog(String type, long hour, String contentId, long countNum) {
        if (StrUtil.isNotEmpty(type) && StrUtil.isNotEmpty(contentId) && countNum > 0) {
            try {
                Db.use().insert(Entity.create(type).set("contentId", contentId).set("date", new Date()).set("hour", hour).set("countNum", countNum));
            } catch (SQLException e) {
                logger.error("插入统计结果到数据库错误 {}", e);
            }
        }
    }

}
