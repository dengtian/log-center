package com.test;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.db.Db;
import cn.hutool.db.Entity;
import cn.hutool.db.Page;
import cn.hutool.db.PageResult;
import cn.hutool.json.JSONUtil;
import com.flink.logcenter.entity.Content;
import com.flink.logcenter.entity.Label;
import com.flink.logcenter.util.*;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class HBaseClientTest {
    public static void main(String[] args) throws IOException, SQLException {

        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.10.0");


        List<String> ids = ContentHBaseUtil.getRelatedContentIds("0273bbac8d2544f3b88837eaa00de25f");
        System.out.println(ids);

    }

    public static void saveTestData() {

        try {

            Label label = new Label("成都", 1);
            Label label2 = new Label("四川", 1);
            Label label3 = new Label("音乐", 1);
            Label label4 = new Label("体育", 1);
            Label label5 = new Label("娱乐", 1);
            Label label6 = new Label("娱乐", 3);
            Label label7 = new Label("娱乐", 10);


            Content content1 = new Content();
            content1.setContentId("001");
            content1.setAuthorId("a001");
            content1.setAuthorArea("成都");
            content1.setFileType("1");
            content1.setLabels(Lists.newArrayList(label, label2, label3));
            content1.setTimestamp(String.valueOf(System.currentTimeMillis()));

            Content content2 = new Content();
            content2.setContentId("002");
            content2.setAuthorId("a002");
            content2.setAuthorArea("成都");
            content2.setFileType("1");
            content2.setLabels(Lists.newArrayList(label2, label3, label4));
            content2.setTimestamp(String.valueOf(System.currentTimeMillis()));


            Content content3 = new Content();
            content3.setContentId("003");
            content3.setAuthorId("a003");
            content3.setAuthorArea("成都");
            content3.setFileType("1");
            content3.setLabels(Lists.newArrayList(label, label2, label3));
            content3.setTimestamp(String.valueOf(System.currentTimeMillis()));

            Content content4 = new Content();
            content4.setContentId("004");
            content4.setAuthorId("a004");
            content4.setAuthorArea("成都");
            content4.setFileType("1");
            content4.setLabels(Lists.newArrayList(label6, label5, label3));
            content4.setTimestamp(String.valueOf(System.currentTimeMillis()));


            save(content1);
            save(content2);
            save(content3);
            save(content4);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static void save(Content content) throws Exception {
        HBaseClient.putData(HBaseConst.CONTENT_TABLE, content.getContentId(), "attr", "contentId", content.getContentId());
        HBaseClient.putData(HBaseConst.CONTENT_TABLE, content.getContentId(), "attr", "timestamp", content.getTimestamp());
        HBaseClient.putData(HBaseConst.CONTENT_TABLE, content.getContentId(), "attr", "fileType", content.getFileType());
        HBaseClient.putData(HBaseConst.CONTENT_TABLE, content.getContentId(), "author", "authorId", content.getAuthorId());
        HBaseClient.putData(HBaseConst.CONTENT_TABLE, content.getContentId(), "author", "authorArea", content.getAuthorArea());


        if (CollUtil.isNotEmpty(content.getLabels())) {
            content.getLabels().forEach(e -> {
                try {
                    HBaseClient.increaseColumn(HBaseConst.CONTENT_TABLE, content.getContentId(), "label", e.getLabel(), e.getWeight());
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            });
        }
    }

    public static void loadContents(int size) throws SQLException {
        PageResult<Entity> result = Db.use().page(Entity.create("core_content"), new Page(1, size));
        if (result.size() > 0) {
            result.forEach(e -> {
                String title = e.getStr("title");
                String content = e.getStr("content");
                List<String> labels = new ArrayList<>();
                if (!StringUtils.isEmpty(title)) {
                    List<String> keys = NlpUtil.getKeysByPredictOnlyWord(title, 5);
                    if (CollectionUtils.isNotEmpty(keys)) {
                        labels.addAll(keys);
                    }
                }
                if (!StringUtils.isEmpty(content)) {
                    List<String> keys = NlpUtil.getKeysByPredictOnlyWord(content, 5);
                    if (CollectionUtils.isNotEmpty(keys)) {
                        labels.addAll(keys);
                    }
                }

                System.out.println(JSONUtil.toJsonStr(labels));

            });
        }
    }
}
