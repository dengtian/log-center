package com.flink.logcenter.util;

import java.util.Calendar;
import java.util.Date;

public class TimeUtil {
    public static long getEndThisHour() {
        Calendar instance = Calendar.getInstance();
        instance.setTime(new Date());
        instance.set(Calendar.HOUR_OF_DAY, instance.get(Calendar.HOUR_OF_DAY) + 1);
        instance.set(Calendar.MINUTE, 0);
        instance.set(Calendar.SECOND, 0);
        return instance.getTime().getTime();
    }


    public static long getHourEndOfTime(long timestamp) {
        Calendar instance = Calendar.getInstance();
        instance.setTime(new Date(timestamp));
        instance.set(Calendar.HOUR_OF_DAY, instance.get(Calendar.HOUR_OF_DAY) + 1);
        instance.set(Calendar.MINUTE, 0);
        instance.set(Calendar.SECOND, 0);
        return instance.getTime().getTime();
    }

}
