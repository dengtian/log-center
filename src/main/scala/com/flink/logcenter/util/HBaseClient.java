package com.flink.logcenter.util;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.setting.Setting;
import com.flink.logcenter.entity.Content;
import com.flink.logcenter.entity.Label;
import com.flink.logcenter.entity.UserLog;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

public class HBaseClient {

    private static Logger log = LoggerFactory.getLogger(HBaseClient.class);


    private static Admin admin;
    public static Connection conn;

    static {
        Configuration conf = HBaseConfiguration.create();
        Setting setting = new Setting("hbase.setting");
        conf.set("hbase.zookeeper.quorum", setting.getStr("hbase.zookeeper.quorum"));
        conf.set("hbase.zookeeper.property.client", "2181");
        conf.set("hbase.rootdir", "hdfs://node1:9000/hbase");
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createTable(String tableName, String... columnFamilies) throws IOException {
        TableName tablename = TableName.valueOf(tableName);
        if (admin.tableExists(tablename)) {
            log.info("Table Exists");
        } else {
            log.info("Start create table");
            HTableDescriptor tableDescriptor = new HTableDescriptor(tablename);
            for (String columnFamliy : columnFamilies) {
                HTableDescriptor column = tableDescriptor.addFamily(new HColumnDescriptor(columnFamliy));
            }
            admin.createTable(tableDescriptor);
            log.info("Create Table success");
        }
    }

    /**
     * 获取一列获取一行数据
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param column
     * @return
     * @throws IOException
     */
    public static String getData(String tableName, String rowKey, String familyName, String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        byte[] row = Bytes.toBytes(rowKey);
        Get get = new Get(row);
        Result result = table.get(get);
        byte[] resultValue = result.getValue(familyName.getBytes(), column.getBytes());
        if (null == resultValue) {
            return null;
        }
        return new String(resultValue);
    }


    /**
     * 获取一行的所有数据
     *
     * @param tableName 表名
     * @param rowKey    列名
     * @throws IOException
     */
    public static List<HBaseCell> getRow(String tableName, String rowKey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        byte[] row = Bytes.toBytes(rowKey);
        Get get = new Get(row);
        Result r = table.get(get);

        List<HBaseCell> cells = new ArrayList<>();

        byte[] rowBytes = r.getRow();
        if (rowBytes == null || rowBytes.length < 1) {
            return Collections.emptyList();
        }
        String key = new String(rowBytes);

        for (Cell cell : r.rawCells()) {
            HBaseCell hBaseCell = new HBaseCell()
                    .setCellKey(key)
                    .setFamily(new String(CellUtil.cloneFamily(cell)))
                    .setQualifier(new String(CellUtil.cloneQualifier(cell)))
                    .setValue(new String(CellUtil.cloneValue(cell)))
                    .setTimestamp(cell.getTimestamp());

            cells.add(hBaseCell);
        }
        return cells;
    }

    /**
     * 向对应列添加数据
     *
     * @param tablename  表名
     * @param rowkey     行号
     * @param famliyname 列族名
     * @param column     列名
     * @param data       数据
     * @throws Exception
     */
    public static void putData(String tablename, String rowkey, String famliyname, String column, String data) throws Exception {
        try {
            if (StrUtil.isNotEmpty(tablename) && StrUtil.isNotEmpty(rowkey) && StrUtil.isNotEmpty(famliyname) && StrUtil.isNotEmpty(data)) {
                Table table = conn.getTable(TableName.valueOf(tablename));
                Put put = new Put(rowkey.getBytes());
                put.addColumn(famliyname.getBytes(), column.getBytes(), data.getBytes());
                table.put(put);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将该单元格加1
     *
     * @param tablename  表名
     * @param rowkey     行号
     * @param famliyname 列族名
     * @param column     列名
     * @throws Exception
     */
    public static void increaseColumn(String tablename, String rowkey, String famliyname, String column, int weight) throws Exception {
        String val = getData(tablename, rowkey, famliyname, column);
        int res = weight;
        if (val != null) {
            res = Integer.valueOf(val) + weight;
        }
        putData(tablename, rowkey, famliyname, column, String.valueOf(res));
    }


    /**
     * 取出表中所有的key
     *
     * @param tableName
     * @return
     */
    public static List<String> getAllKey(String tableName) throws IOException {
        List<String> keys = new ArrayList<>();
        Scan scan = new Scan();
        Table table = conn.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
            keys.add(new String(r.getRow()));
        }
        return keys;
    }

    /**
     * 根据时间戳传所有key
     */

    public static List<UserLog> getAllKeyByTime(String tableName, long minStamp, long maxStamp) throws IOException {
        List<UserLog> list = new ArrayList<>();
        Scan scan = new Scan();
        scan.setTimeRange(minStamp, maxStamp);
        Table table = conn.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
            String key = new String(r.getRow());
            UserLog log = new UserLog();
            log.setUserId(key);
            List<String> dataIds = new LinkedList<>();
            for (Cell cell : r.rawCells()) {
                HBaseCell hBaseCell = new HBaseCell()
                        .setCellKey(key)
                        .setFamily(new String(CellUtil.cloneFamily(cell)))
                        .setQualifier(new String(CellUtil.cloneQualifier(cell)))
                        .setValue(new String(CellUtil.cloneValue(cell)))
                        .setTimestamp(cell.getTimestamp());
                if ("data".equals(hBaseCell.getFamily())) {
                    dataIds.add(hBaseCell.getValue());
                }
            }
            log.setDataIds(dataIds);
            list.add(log);
        }

        return list;
    }

    /**
     * 查询包含指定列名的key
     */

    public static List<String> getKeysByColumn(String tableName, String columnName) throws IOException {

        if (StrUtil.isNotEmpty(tableName) && StrUtil.isNotEmpty(columnName)) {
            Scan scan = new Scan();
            List<String> keys = new ArrayList<>();
            Filter filter = new QualifierFilter(CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(Bytes.toBytes(columnName)));
            scan.setFilter(filter);
            Table table = conn.getTable(TableName.valueOf(tableName));
            ResultScanner scanner = table.getScanner(scan);
            for (Result r : scanner) {
                keys.add(new String(r.getRow()));
            }
        }

        return Collections.emptyList();
    }

    public static List<Content> getKeysByFilter(String tableName, Filter filter) throws IOException {

        Scan scan = new Scan();
        scan.setFilter(filter);
        Table table = conn.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = table.getScanner(scan);
        List<Content> contents = new ArrayList<>();

        scanner.forEach(r -> {
            Content content = new Content();
            content.setContentId(new String(r.getRow()));
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
                if (StrUtil.isNotEmpty(qualifier)) {
                    switch (qualifier) {
                        case "fileType": {
                            content.setFileType(value);
                            break;
                        }

                        case "timestamp": {
                            content.setTimestamp(value);
                            break;
                        }

                        case "authorArea": {
                            content.setAuthorArea(value);
                            break;
                        }

                        case "authorId": {
                            content.setAuthorId(value);
                            break;
                        }

                        default:
                            break;
                    }

                    if (StrUtil.isNotEmpty(family) && "label".equals(family)) {
                        labels.add(new Label(qualifier, Integer.valueOf(value)));
                    }
                }
                content.setLabels(labels);
            }
            contents.add(content);
        });

        return contents;
    }


    /**
     * 获得相等过滤器。相当于SQL的 [字段] = [值]
     *
     * @param cf  列族名
     * @param col 列名
     * @param val 值
     * @return 过滤器
     */
    public static Filter eqFilter(String cf, String col, byte[] val) {
        SingleColumnValueFilter f = new SingleColumnValueFilter(cf.getBytes(), col.getBytes(), CompareFilter.CompareOp.EQUAL, val);
        f.setLatestVersionOnly(true);
        f.setFilterIfMissing(true);
        return f;
    }

    /**
     * 获得大于过滤器。相当于SQL的 [字段] > [值]
     *
     * @param cf  列族名
     * @param col 列名
     * @param val 值
     * @return 过滤器
     */
    public static Filter gtFilter(String cf, String col, byte[] val) {
        SingleColumnValueFilter f = new SingleColumnValueFilter(cf.getBytes(), col.getBytes(), CompareFilter.CompareOp.GREATER, val);
        f.setLatestVersionOnly(true);
        f.setFilterIfMissing(true);
        return f;
    }

    /**
     * 获得大于等于过滤器。相当于SQL的 [字段] >= [值]
     *
     * @param cf  列族名
     * @param col 列名
     * @param val 值
     * @return 过滤器
     */
    public static Filter gtEqFilter(String cf, String col, byte[] val) {
        SingleColumnValueFilter f = new SingleColumnValueFilter(cf.getBytes(), col.getBytes(), CompareFilter.CompareOp.GREATER_OR_EQUAL, val);
        f.setLatestVersionOnly(true);
        f.setFilterIfMissing(true);
        return f;
    }

    /**
     * 获得小于过滤器。相当于SQL的 [字段] < [值]
     *
     * @param cf  列族名
     * @param col 列名
     * @param val 值
     * @return 过滤器
     */
    public static Filter ltFilter(String cf, String col, byte[] val) {
        SingleColumnValueFilter f = new SingleColumnValueFilter(cf.getBytes(), col.getBytes(), CompareFilter.CompareOp.LESS, val);
        f.setLatestVersionOnly(true);
        f.setFilterIfMissing(true);
        return f;
    }

    /**
     * 获得小于等于过滤器。相当于SQL的 [字段] <= [值]
     *
     * @param cf  列族名
     * @param col 列名
     * @param val 值
     * @return 过滤器
     */
    public static Filter lteqFilter(String cf, String col, byte[] val) {
        SingleColumnValueFilter f = new SingleColumnValueFilter(cf.getBytes(), col.getBytes(), CompareFilter.CompareOp.LESS_OR_EQUAL, val);
        f.setLatestVersionOnly(true);
        f.setFilterIfMissing(true);
        return f;
    }

    /**
     * 获得不等于过滤器。相当于SQL的 [字段] != [值]
     *
     * @param cf  列族名
     * @param col 列名
     * @param val 值
     * @return 过滤器
     */
    public static Filter neqFilter(String cf, String col, byte[] val) {
        SingleColumnValueFilter f = new SingleColumnValueFilter(cf.getBytes(), col.getBytes(), CompareFilter.CompareOp.NOT_EQUAL, val);
        f.setLatestVersionOnly(true);
        f.setFilterIfMissing(true);
        return f;
    }

    /**
     * 和过滤器 相当于SQL的 的 and
     *
     * @param filters 多个过滤器
     * @return 过滤器
     */
    public static Filter andFilter(Filter... filters) {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        if (filters != null && filters.length > 0) {
            if (filters.length > 1) {
                for (Filter f : filters) {
                    filterList.addFilter(f);
                }
            }
            if (filters.length == 1) {
                return filters[0];
            }
        }
        return filterList;
    }

    /**
     * 和过滤器 相当于SQL的 的 and
     *
     * @param filters 多个过滤器
     * @return 过滤器
     */
    public static Filter andFilter(Collection<Filter> filters) {
        return andFilter(filters.toArray(new Filter[0]));
    }


    /**
     * 或过滤器 相当于SQL的 or
     *
     * @param filters 多个过滤器
     * @return 过滤器
     */
    public static Filter orFilter(Filter... filters) {
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        if (filters != null && filters.length > 0) {
            for (Filter f : filters) {
                filterList.addFilter(f);
            }
        }
        return filterList;
    }

    /**
     * 或过滤器 相当于SQL的 or
     *
     * @param filters 多个过滤器
     * @return 过滤器
     */
    public static Filter orFilter(Collection<Filter> filters) {
        return orFilter(filters.toArray(new Filter[0]));
    }

    /**
     * 非空过滤器 相当于SQL的 is not null
     *
     * @param cf  列族
     * @param col 列
     * @return 过滤器
     */
    public static Filter notNullFilter(String cf, String col) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(cf.getBytes(), col.getBytes(), CompareFilter.CompareOp.NOT_EQUAL, new NullComparator());
        filter.setFilterIfMissing(true);
        filter.setLatestVersionOnly(true);
        return filter;
    }

    /**
     * 空过滤器 相当于SQL的 is null
     *
     * @param cf  列族
     * @param col 列
     * @return 过滤器
     */
    public static Filter nullFilter(String cf, String col) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(cf.getBytes(), col.getBytes(), CompareFilter.CompareOp.EQUAL, new NullComparator());
        filter.setFilterIfMissing(false);
        filter.setLatestVersionOnly(true);
        return filter;
    }

    /**
     * 子字符串过滤器 相当于SQL的 like '%[val]%'
     *
     * @param cf  列族
     * @param col 列
     * @param sub 子字符串
     * @return 过滤器
     */
    public static Filter subStringFilter(String cf, String col, String sub) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(cf.getBytes(), col.getBytes(), CompareFilter.CompareOp.EQUAL, new SubstringComparator(sub));
        filter.setFilterIfMissing(true);
        filter.setLatestVersionOnly(true);
        return filter;
    }

    /**
     * 正则过滤器 相当于SQL的 rlike '[regex]'
     *
     * @param cf    列族
     * @param col   列
     * @param regex 正则表达式
     * @return 过滤器
     */
    public static Filter regexFilter(String cf, String col, String regex) {
        SingleColumnValueFilter filter = new SingleColumnValueFilter(cf.getBytes(), col.getBytes(), CompareFilter.CompareOp.EQUAL, new RegexStringComparator(regex));
        filter.setFilterIfMissing(true);
        filter.setLatestVersionOnly(true);
        return filter;
    }

    /**
     * 删除指定表的某个列值
     *
     * @param tableName
     * @param rowKey
     * @param family
     * @param qualifier
     * @throws IOException
     */
    public static void deleteData(String tableName, String rowKey, String family, String qualifier) throws IOException {
        TableName name = TableName.valueOf(tableName);
        Table table = conn.getTable(name);
        Delete del = new Delete(Bytes.toBytes(rowKey));
        del.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        table.delete(del);
    }


    public static void deleteByRowKey(String tableName, String rowKey) throws Exception {
        if (StrUtil.isNotEmpty(tableName) && StrUtil.isNotEmpty(rowKey)) {
            TableName name = TableName.valueOf(tableName);
            Table table = conn.getTable(name);
            Delete del = new Delete(Bytes.toBytes(rowKey));
            table.delete(del);
        }
    }

    public static Content getContent(String id) throws IOException {

        Table table = conn.getTable(TableName.valueOf(HBaseConst.CONTENT_TABLE));
        byte[] row = Bytes.toBytes(id);
        Get get = new Get(row);
        Result r = table.get(get);

        Content content = new Content();


        List<Cell> cells = r.listCells();
        List<Label> labels = new ArrayList<>();

        if (CollUtil.isEmpty(cells)) {
            return null;
        }

        for (Cell cell : cells) {
            HBaseCell hBaseCell = new HBaseCell()
                    .setCellKey(CellUtil.getCellKeyAsString(cell))
                    .setFamily(new String(CellUtil.cloneFamily(cell)))
                    .setQualifier(new String(CellUtil.cloneQualifier(cell)))
                    .setValue(new String(CellUtil.cloneValue(cell)))
                    .setTimestamp(cell.getTimestamp());

            String qualifier = hBaseCell.getQualifier();
            String family = hBaseCell.getFamily();
            String value = hBaseCell.getValue();
            if (StrUtil.isNotEmpty(qualifier)) {
                switch (qualifier) {
                    case "fileType": {
                        content.setFileType(value);
                        break;
                    }

                    case "timestamp": {
                        content.setTimestamp(value);
                        break;
                    }

                    case "authorArea": {
                        content.setAuthorArea(value);
                        break;
                    }

                    case "authorId": {
                        content.setAuthorId(value);
                        break;
                    }
                    case "contentId": {
                        content.setContentId(value);
                        break;
                    }


                    default:
                        break;
                }

                if (StrUtil.isNotEmpty(family) && "label".equals(family)) {
                    labels.add(new Label(qualifier, Integer.valueOf(value)));
                }
            }
        }

        content.setLabels(labels);

        return content;
    }


    public static ResultScanner getAll(String tableName) throws IOException {
        Scan scan = new Scan();
        Table table = conn.getTable(TableName.valueOf(tableName));
        return table.getScanner(scan);
    }

    public static Object getByFilter(String tableName, Filter filter) throws IOException {

        Scan scan = new Scan();
        scan.setFilter(filter);
        Table table = conn.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = table.getScanner(scan);
        List<HBaseCell> cells = new LinkedList<>();
        scanner.forEach(r -> {
            for (Cell cell : r.rawCells()) {
                HBaseCell hBaseCell = new HBaseCell()
                        .setCellKey(CellUtil.getCellKeyAsString(cell))
                        .setFamily(new String(CellUtil.cloneFamily(cell)))
                        .setQualifier(new String(CellUtil.cloneQualifier(cell)))
                        .setValue(new String(CellUtil.cloneValue(cell)))
                        .setTimestamp(cell.getTimestamp());
                cells.add(hBaseCell);

            }
        });

        return cells;
    }
}
