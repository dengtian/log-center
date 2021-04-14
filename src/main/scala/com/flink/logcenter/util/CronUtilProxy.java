package com.flink.logcenter.util;

import cn.hutool.cron.CronUtil;
import cn.hutool.cron.task.Task;

public class CronUtilProxy {
    public static String schedule(String schedulingPattern, Task task) {
        return CronUtil.schedule(schedulingPattern, task);
    }
}
