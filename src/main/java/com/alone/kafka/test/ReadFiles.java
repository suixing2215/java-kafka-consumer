package com.alone.kafka.test;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.alone.kafka.entry.AlarmMessage;
import com.alone.kafka.utils.DBUtils;
import lombok.SneakyThrows;

import java.io.*;
import java.lang.reflect.Field;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @author Administrator
 */
public class ReadFiles {
    private final static DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");
    private final static DateTimeFormatter MONTH_FORMATTER = DateTimeFormatter.ofPattern("yyyyMM");

    @SneakyThrows
    public static void main(String[] args) {
        List<String> list = txt2list("C:\\Users\\Administrator\\Desktop\\new 5.txt");
        List<Map<String, Object>> dataList = new ArrayList<>();
        assert list != null;
        if (!list.isEmpty()) {
            for (String s : list) {
                if (!isJsonValidate(s)) {
                    continue;
                }
                AlarmMessage alarmMessage = JSONObject.parseObject(s, AlarmMessage.class);
                if (null == (alarmMessage.getEventTime())) {
                    continue;
                }
                System.out.println("------------------------------------");
                LocalDateTime dateTime = dateToLocalDate(alarmMessage.getEventTime());
                if (null==dateTime){
                    continue;
                }
                alarmMessage.setDt_month(MONTH_FORMATTER.format(Objects.requireNonNull(dateTime)));
                alarmMessage.setDt_day(DATE_TIME_FORMATTER.format(Objects.requireNonNull(dateTime)));
                System.out.println(alarmMessage.getDt_month());
                System.out.println(alarmMessage.getDt_day());
                Map<String, Object> map = getObjectToMap(alarmMessage);
                dataList.add(map);
            }
        }
        DBUtils.insertAllByList("alarm_message_tmp", dataList, getList());
    }

    public static List<String> txt2list(String path) {
        List<String> list = new ArrayList<>();
        File file = new File(path);
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String s = null;
            while ((s = bufferedReader.readLine()) != null) {
                list.add(s);
            }
            bufferedReader.close();
            return list;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }

    }

    /**
     * Object转Map
     */
    public static Map<String, Object> getObjectToMap(Object obj) throws IllegalAccessException {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        Class<?> clazz = obj.getClass();
        for (Field field : clazz.getDeclaredFields()) {
            field.setAccessible(true);
            String fieldName = field.getName();
            if ("insertTime".equals(fieldName)){
                continue;
            }
            if ("updateTime".equals(fieldName)){
                continue;
            }
            Object value = field.get(obj);
            if (value == null) {
                value = "";
            }
            map.put(fieldName, value);
        }
        return map;
    }

    public static List<String> getList() {
        List<String> cols = new ArrayList<>();
        cols.add("IntVersion");
        cols.add("MsgSerial");
        cols.add("AlarmUniqueId");
        cols.add("NeId");
        cols.add("NeName");
        cols.add("SystemName");
        cols.add("EquipmentClass");
        cols.add("Vendor");
        cols.add("LocateNeName");
        cols.add("LocateNeType");
        cols.add("EventTime");
        cols.add("CancelTime");
        cols.add("DalTime");
        cols.add("VendorAlarmType");
        cols.add("VendorSeverity");
        cols.add("AlarmSeverity");
        cols.add("VendorAlarmId");
        cols.add("AlarmStatus");
        cols.add("AlarmTitle");
        cols.add("ProbableCauseTxt");
        cols.add("AlarmLogicClass");
        cols.add("AlarmLogicSubClass");
        cols.add("EffectOnEquipment");
        cols.add("EffectOnBusiness");
        cols.add("NmsAlarmType");
        cols.add("AlarmProvince");
        cols.add("AlarmRegion");
        cols.add("AlarmCounty");
        cols.add("dt_day");
        cols.add("dt_month");
        return cols;
    }

    public static boolean isJsonValidate(String log) {
        try {
            JSON.parse(log);
            return true;
        } catch (JSONException e) {
            return false;
        }
    }

    /**
     *  Date 转化成 LocalDateTime
     */
    public static LocalDateTime dateToLocalDate(Date date) {
        try {
            Instant instant = date.toInstant();
            ZoneId zoneId = ZoneId.systemDefault();
            return instant.atZone(zoneId).toLocalDateTime();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

}