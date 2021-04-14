package com.flink.logcenter.util;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateTime;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.NumberUtil;
import cn.hutool.core.util.StrUtil;
import com.flink.logcenter.entity.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ContentHBaseUtil {

    /**
     * 获取最近N天的内容
     *
     * @param days
     * @return
     */
    public static List<Content> getContents(int days) {
        DateTime dateTime = DateUtil.offsetDay(new Date(), -days);
        long time = dateTime.getTime();
        try {
            return HBaseClient.getKeysByFilter(HBaseConst.CONTENT_TABLE, HBaseClient.gtEqFilter("attr", "timestamp", String.valueOf(time).getBytes()));
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }


    /**
     * 获取内容关联度最高的内容
     *
     * @param content
     * @return
     */
    public static Map<String, Double> getMaxCoeffContents(Content content, int size, int days) {
        List<Content> contents = getContents(days);
        Map<String, Double> map = new HashMap<>();
        Map<String, Double> max = new LinkedHashMap<>();
        if (CollUtil.isNotEmpty(contents)) {
            contents.stream().filter(e -> StrUtil.isNotEmpty(e.getContentId()) && !e.getContentId().equals(content.getContentId())).forEach(e -> {
                double labelScore = ContentCoeff.getLabelScore(content, e);
                map.put(e.getContentId(), labelScore);
            });
            map.entrySet()
                    .stream()
                    .sorted((p1, p2) -> p2.getValue().compareTo(p1.getValue()))
                    .collect(Collectors.toList())
                    .forEach(e -> {
                        if (max.size() < size) {
                            max.put(e.getKey(), e.getValue());
                        }
                    });
        }

        return max;
    }


    /**
     * 保存内容关联
     *
     * @param contentId
     * @param coeff
     */
    public static void saveContentCoeffRelate(String contentId, Map<String, Double> coeff) {
        if (StrUtil.isNotEmpty(contentId) && CollUtil.isNotEmpty(coeff)) {
            try {
                coeff.forEach((k, v) -> {
                    try {
                        HBaseClient.deleteByRowKey(HBaseConst.CONTENT_RELATE_TABLE, contentId);
                        HBaseClient.putData(HBaseConst.CONTENT_RELATE_TABLE, contentId, "relate", k, v.toString());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 保存协同过滤关联
     *
     * @param contentId
     * @param coeff
     */
    public static void saveItemCoeffRelate(String contentId, Map<String, Double> coeff) {

        if (StrUtil.isNotEmpty(contentId) && CollUtil.isNotEmpty(coeff)) {
            try {
                HBaseClient.deleteByRowKey(HBaseConst.ITEM_COEFF_TABLE, contentId);
                coeff.forEach((k, v) -> {
                    try {
                        HBaseClient.putData(HBaseConst.ITEM_COEFF_TABLE, contentId, "relate", k, v.toString());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 计算关联度并保存
     */

    public static void calculateCoeffAndSave(Content content, int size, int days) {
        if (content != null && size > 0 & days > 0) {
            List<Content> contents = getContents(days);
            Map<String, Double> map = new HashMap<>();
            Map<String, Double> max = new LinkedHashMap<>();
            if (CollUtil.isNotEmpty(contents)) {
                contents.stream().filter(e -> StrUtil.isNotEmpty(e.getContentId()) && !e.getContentId().equals(content.getContentId())).forEach(e -> {
                    double labelScore = ContentCoeff.getLabelScore(content, e);
                    if (labelScore > 0d) {
                        map.put(e.getContentId(), labelScore);
                    }
                });
                map.entrySet()
                        .stream().filter(e -> e.getValue() > 0d)
                        .sorted((p1, p2) -> p2.getValue().compareTo(p1.getValue()))
                        .collect(Collectors.toList())
                        .forEach(e -> {
                            if (max.size() < size) {
                                max.put(e.getKey(), e.getValue());
                            }
                        });
            }

            if (CollUtil.isNotEmpty(max)) {
                saveContentCoeffRelate(content.getContentId(), max);
            }
            if (CollUtil.isNotEmpty(map)) {
                contents.forEach(c -> {
                    Double aDouble = map.get(c.getContentId());
                    if (aDouble != null && aDouble > 0L) {
                        List<HBaseCell> row = getCoeffData(c.getContentId());
                        if (CollUtil.isNotEmpty(row)) {
                            List<CoeffRelate> coeffRelates = new ArrayList<>();
                            row.forEach(e -> coeffRelates.add(new CoeffRelate(e.getQualifier(), Double.valueOf(e.getValue()))));
                            coeffRelates.sort(Comparator.comparing(CoeffRelate::getScore));
                            CoeffRelate min = coeffRelates.get(row.size() - 1);
                            if (min.getScore() < aDouble) {
                                try {
                                    HBaseClient.deleteData(HBaseConst.CONTENT_RELATE_TABLE, c.getContentId(), "relate", min.getKey());
                                    HBaseClient.putData(HBaseConst.CONTENT_RELATE_TABLE, c.getContentId(), "relate", content.getContentId(), aDouble.toString());
                                } catch (Exception e) {
                                    e.printStackTrace();
                                }
                            }
                        } else {
                            try {
                                HBaseClient.putData(HBaseConst.CONTENT_RELATE_TABLE, c.getContentId(), "relate", content.getContentId(), aDouble.toString());
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                });
            }
        }
    }

    public static void calculateRelate(Content content, List<Content> contents) {
        if (content != null && CollUtil.isNotEmpty(contents)) {
            Map<String, Double> map = new HashMap<>();
            Map<String, Double> max = new LinkedHashMap<>();
            if (CollUtil.isNotEmpty(contents)) {
                contents.stream().filter(e -> StrUtil.isNotEmpty(e.getContentId()) && !e.getContentId().equals(content.getContentId())).forEach(e -> {
                    double labelScore = ContentCoeff.getLabelScore(content, e);
                    if (labelScore > 0d) {
                        map.put(e.getContentId(), labelScore);
                    }
                });
                map.entrySet()
                        .stream().filter(e -> e.getValue() > 0d)
                        .sorted((p1, p2) -> p2.getValue().compareTo(p1.getValue()))
                        .collect(Collectors.toList())
                        .forEach(e -> {
                            if (max.size() < 20) {
                                max.put(e.getKey(), e.getValue());
                            }
                        });
            }

            if (CollUtil.isNotEmpty(max)) {
                saveContentCoeffRelate(content.getContentId(), max);
            }

        }
    }


    /**
     * 根据稿件id获取关联表数据
     */
    public static List<HBaseCell> getCoeffData(String contentId) {
        try {
            return HBaseClient.getRow(HBaseConst.CONTENT_RELATE_TABLE, contentId);
        } catch (IOException e) {
            e.printStackTrace();
            return Collections.emptyList();
        }
    }

    public static void saveUserContentLog(String userId, String contentId, int weight) {
        if (StrUtil.isNotEmpty(userId) && StrUtil.isNotEmpty(contentId) && weight > 0) {
            try {
                HBaseClient.increaseColumn(HBaseConst.USER_CONTENT_HISTORY_TABLE, userId, "p", contentId, weight);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void saveContentUserLog(String contentId, String userId, int weight) {
        if (StrUtil.isNotEmpty(userId) && StrUtil.isNotEmpty(contentId) && weight > 0) {
            try {
                HBaseClient.increaseColumn(HBaseConst.CONTENT_USER_HISTORY_TABLE, contentId, "p", userId, weight);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void increaseUserLabel(String logType, String userId, String dataId) {
        if (StrUtil.isNotEmpty(logType) && StrUtil.isNotEmpty(userId) && StrUtil.isNotEmpty(dataId)) {
            DataLabels dataLabel = getDataLabel(dataId, logType);
            if (dataLabel != null) {
                if (CollUtil.isNotEmpty(dataLabel.getLabels())) {
                    dataLabel.getLabels().forEach(label -> {
                        try {
                            HBaseClient.increaseColumn(HBaseConst.USER_LABEL, userId, logType, label.getLabel(), 1);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });
                }
            }
        }
    }

    public static Map<String, Set<Label>> getUserLabels(String userId) {

        if (StrUtil.isNotEmpty(userId)) {
            try {
                List<HBaseCell> cells = HBaseClient.getRow(HBaseConst.USER_LABEL, userId);
                if (CollUtil.isNotEmpty(cells)) {
                    Map<String, Set<Label>> map = new HashMap<>();
                    cells.forEach(cell -> {
                        String family = cell.getFamily();
                        String qualifier = cell.getQualifier();
                        String value = cell.getValue();
                        if (StrUtil.isNotEmpty(family) && StrUtil.isNotEmpty(qualifier)) {
                            if (StrUtil.isEmpty(value)) {
                                value = "1";
                            }
                            Integer weight = Integer.valueOf(value);
                            Set<Label> labels = map.get(family);
                            Label label = new Label(qualifier, weight);
                            if (labels != null) {
                                labels.add(label);
                            } else {
                                Set<Label> set = new HashSet<>();
                                set.add(label);
                                map.put(family, set);
                            }
                        }
                    });
                    return map;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return Collections.emptyMap();
    }


    //查询data_labels中的所有数据
    public static List<DataLabels> getAllDataLabels() {
        try {
            ResultScanner resultScanner = HBaseClient.getAll(HBaseConst.DATA_LABELS);
            if (resultScanner != null) {
                List<DataLabels> list = new ArrayList<>();
                for (Result r : resultScanner) {
                    DataLabels dataLabels = new DataLabels();
                    String s = new String(r.getRow());
                    dataLabels.setDataId(s);
                    List<Label> labels = new ArrayList<>();
                    for (Cell cell : r.rawCells()) {
                        HBaseCell hBaseCell = new HBaseCell()
                                .setCellKey(CellUtil.getCellKeyAsString(cell))
                                .setFamily(new String(CellUtil.cloneFamily(cell)))
                                .setQualifier(new String(CellUtil.cloneQualifier(cell)))
                                .setValue(new String(CellUtil.cloneValue(cell)))
                                .setTimestamp(cell.getTimestamp());

                        String qualifier = hBaseCell.getQualifier();
                        String family = hBaseCell.getFamily();
                        String value = hBaseCell.getValue();


                        if (StrUtil.isNotEmpty(family) && StrUtil.isNotEmpty(qualifier)) {
                            if ("label".equalsIgnoreCase(family)) {
                                if (StrUtil.isEmpty(value)) {
                                    value = "1";
                                }
                                if (NumberUtil.isNumber(value)) {
                                    labels.add(new Label(qualifier, Integer.valueOf(value)));
                                }
                            } else {
                                if ("type".equalsIgnoreCase(family) && "name".equalsIgnoreCase(qualifier)) {
                                    dataLabels.setType(value);
                                }
                            }
                        }

                    }
                    dataLabels.setLabels(labels);
                    list.add(dataLabels);
                }
                return list;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return Collections.emptyList();
    }

    public static void getDataIdByType(String userId, String type, int size) {
        if (StrUtil.isNotEmpty(userId) && StrUtil.isNotEmpty(type) && size > 0) {

        }
    }

    public static DataLabels getDataLabel(String dataId, String type) {
        if (StrUtil.isNotEmpty(dataId) && StrUtil.isNotEmpty(type)) {
            DataLabels dataLabels = new DataLabels();
            try {
                List<HBaseCell> cells = HBaseClient.getRow(HBaseConst.DATA_LABELS, type + "_" + dataId);
                List<Label> labels = new LinkedList<>();

                cells.forEach(cell -> {

                    String family = cell.getFamily();
                    String value = cell.getValue();
                    String qualifier = cell.getQualifier();
                    String cellKey = cell.getCellKey();
                    dataLabels.setDataId(StrUtil.removePrefix(cellKey, type + "_"));
                    if ("label".equalsIgnoreCase(family) && NumberUtil.isNumber(value)) {
                        labels.add(new Label(qualifier, Integer.valueOf(value)));
                    } else if ("type".equalsIgnoreCase(family) && "name".equalsIgnoreCase(qualifier)) {
                        dataLabels.setType(value);
                    }
                });

                dataLabels.setLabels(labels);
            } catch (IOException e) {
                e.printStackTrace();
            }

            return dataLabels;
        } else {
            return null;
        }
    }


    public static void saveDataLabels(String dataId, String type, List<Label> labels) {
        if (StrUtil.isNotEmpty(dataId) && StrUtil.isNotEmpty(type) && CollUtil.isNotEmpty(labels)) {
            try {
                HBaseClient.putData(HBaseConst.DATA_LABELS, type + "_" + dataId, "type", "name", type);

                labels.forEach(label -> {
                    try {
                        HBaseClient.putData(HBaseConst.DATA_LABELS, type + "_" + dataId, "label", label.getLabel(), String.valueOf(label.getWeight()));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static List<ContentUserLog> getContentUserLog() {
        try {
            ResultScanner scanner = HBaseClient.getAll(HBaseConst.CONTENT_USER_HISTORY_TABLE);
            List<ContentUserLog> logs = new ArrayList<>();
            if (scanner != null) {
                for (Result r : scanner) {
                    for (Cell cell : r.rawCells()) {
                        ContentUserLog log = new ContentUserLog();
                        log.setContentId(new String(r.getRow()));
                        HBaseCell hBaseCell = new HBaseCell()
                                .setCellKey(CellUtil.getCellKeyAsString(cell))
                                .setFamily(new String(CellUtil.cloneFamily(cell)))
                                .setQualifier(new String(CellUtil.cloneQualifier(cell)))
                                .setValue(new String(CellUtil.cloneValue(cell)))
                                .setTimestamp(cell.getTimestamp());

                        String qualifier = hBaseCell.getQualifier();
                        String family = hBaseCell.getFamily();
                        String value = hBaseCell.getValue();
                        if (StrUtil.isNotEmpty(family) && StrUtil.isNotEmpty(qualifier)) {
                            if ("p".equals(family)) {
                                log.setUserId(qualifier);
                            }
                        }
                        logs.add(log);
                    }
                }
            }
            return logs;

        } catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
    }

    /**
     * 查询相似稿件
     *
     * @param contentId
     * @return
     */
    public static List<String> getRelatedContentIds(String contentId) {
        if (StrUtil.isNotEmpty(contentId)) {
            try {
                List<HBaseCell> rows = HBaseClient.getRow(HBaseConst.CONTENT_RELATE_TABLE, contentId);
                if (CollUtil.isNotEmpty(rows)) {
                    List<String> ids = new LinkedList<>();
                    rows.forEach(r -> {
                        if (r.getFamily().equalsIgnoreCase("relate")
                                && StrUtil.isNotEmpty(r.getQualifier())
                                && StrUtil.isNotEmpty(r.getValue())
                                && NumberUtil.isNumber(r.getValue())
                                && Double.valueOf(r.getValue()) > 0) {
                            ids.add(r.getQualifier());
                        }
                    });
                    return ids;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return Collections.emptyList();
    }

    /**
     * 查询看过@contentId的用户也看过哪些稿件
     *
     * @param contentId
     * @return
     */
    public static List<String> getItemCoeffContentIds(String contentId) {
        if (StrUtil.isNotEmpty(contentId)) {
            try {
                List<HBaseCell> rows = HBaseClient.getRow(HBaseConst.ITEM_COEFF_TABLE, contentId);
                if (CollUtil.isNotEmpty(rows)) {
                    List<String> ids = new LinkedList<>();
                    rows.forEach(r -> {
                        if (r.getFamily().equalsIgnoreCase("relate")
                                && StrUtil.isNotEmpty(r.getQualifier())
                                && StrUtil.isNotEmpty(r.getValue())
                                && NumberUtil.isNumber(r.getValue())
                                && Double.valueOf(r.getValue()) > 0) {
                            ids.add(r.getQualifier());
                        }
                    });
                    return ids;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        return Collections.emptyList();
    }

}
