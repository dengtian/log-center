package com.flink.logcenter.util;

import cn.hutool.core.collection.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AreaUtil {

    private static Logger logger = LoggerFactory.getLogger(AreaUtil.class);


    public static final Map<String, String> province = new HashMap<>();
    public static final Map<String, String> district = new HashMap<>();
    public static final Map<String, Area> map = new HashMap<>();


    public static Area getByCode(String code) {
        if (map.isEmpty()) {
            synchronized (AreaUtil.class) {
                getList();
            }
        }
        return map.get(code);
    }

    private static void getList() {


        String line;
        BufferedReader reader = null;

        String cityCode = "";
        String countyCode = "";
        List<Area> result = new ArrayList<>();

        try {
            InputStream resourceAsStream = AreaUtil.class.getResourceAsStream("/code.txt");
            if (resourceAsStream == null) {
                return;
            }
            InputStreamReader input = new InputStreamReader(resourceAsStream, "UTF-8");
            if (input == null) {
                return;
            }
            reader = new BufferedReader(new BufferedReader(input));

            //读取文件的每一行
            while ((line = reader.readLine()) != null) {
                String[] data = doString(line);
                data[0] = data[0].trim();
                data[1] = data[1].trim();

                //处理读取的文件记录
                if (isSheng(data[0])) {
                    cityCode = data[0];
                    Area area = new Area(data[0], data[1], 0, "0");
                    province.put(data[0], data[1]);
                    result.add(area);
                } else if (isShi(data[0])) {
                    countyCode = data[0];
                    Area area = new Area(data[0], data[1], 1, cityCode);
                    district.put(data[0], data[1]);
                    result.add(area);
                } else {
                    Area area = new Area(data[0], data[1], 2, countyCode);
                    area.setCounty(data[1]);
                    result.add(area);
                }

            }

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        if (CollectionUtil.isNotEmpty(result)) {
            result.forEach(e -> {
                if (e.getLevel() == 0) {
                    e.setProvince(e.getName());
                }
                if (e.getLevel() == 1) {
                    String code = e.getCode();
                    String provinceCode = code.substring(0, 2) + "0000";
                    String provinceName = province.get(provinceCode);
                    e.setProvince(provinceName);
                }
                if (e.getLevel() == 2) {
                    String code = e.getCode();
                    String provinceCode = code.substring(0, 2) + "0000";
                    String provinceName = province.get(provinceCode);
                    e.setProvince(provinceName);
                    String districtCode = code.substring(0, 4) + "00";
                    String districtName = district.get(districtCode);
                    e.setDistrict(districtName);
                }
                map.put(e.getCode(), e);
            });
        }
    }

    //字符分割
    private static String[] doString(String line) {
        String code = "";
        String name = "";
        code = line.substring(0, 6);
        name = line.substring(6, line.length());
        String[] result = new String[]{code, name};
        return result;
    }

    //判断是否省或者直辖市
    private static boolean isSheng(String code) {
        String last = code.substring(2);
        if ("0000".equalsIgnoreCase(last)) {
            return true;
        }
        return false;

    }

    //判断是否地级市
    private static boolean isShi(String code) {
        String last = code.substring(4);
        if ("00".equalsIgnoreCase(last)) {
            return true;
        }
        return false;
    }


    public static void main(String[] args) {
        Area byCode = getByCode("510000");
        System.out.println(byCode.getProvince());
    }

}
