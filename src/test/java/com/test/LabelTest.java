package com.test;

import cn.hutool.json.JSONUtil;
import com.flink.logcenter.util.NlpUtil;

import java.util.List;

public class LabelTest {
    public static void main(String[] args) {
        List<String> list = NlpUtil.getKeysByPredictOnlyWord("对外开放项目攻坚：蓬安在共享电动车的投放问题上应该怎么办？", 10);
        System.out.println(JSONUtil.toJsonStr(list));
    }
}
