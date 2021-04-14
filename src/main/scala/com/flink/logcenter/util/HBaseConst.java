package com.flink.logcenter.util;

public class HBaseConst {
    //稿件表
    public static String CONTENT_TABLE = "content";
    //稿件标签关联表
    public static String CONTENT_RELATE_TABLE = "content_relate";
    //协同过滤关联表
    public static String ITEM_COEFF_TABLE = "item_coeff";
    //用户-稿件历史记录
    public static String USER_CONTENT_HISTORY_TABLE = "user_content_history";
    //稿件-用户历史记录
    public static String CONTENT_USER_HISTORY_TABLE = "content_user_history";
    //用户标签表
    public static String USER_LABEL = "user_label";
    public static String DATA_LABELS = "data_labels";
    public static String USER_LOG = "user_log";
    public static String USER_RELATE = "user_relate";
}
