package com.alone.kafka.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @program: stand-alone-kafka
 * @description: 工具类
 * @author: liyang
 * @create: 2021/10/22 10:01
 */
public class oConvertUtils {

    private final static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private final static DateTimeFormatter MONTH_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM");
    private final static DateTimeFormatter HOUR_FORMATTER = DateTimeFormatter.ofPattern("HH");
    private final static DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static List<String> getOLTList() {
        List<String> cols = new ArrayList<>();
        cols.add("ProvinceID");
        cols.add("IntVersion");
        cols.add("MsgSerial");
        cols.add("AlarmUniqueId");
        cols.add("ClearId");
        cols.add("StandardFlag");
        cols.add("SubAlarmType");
        cols.add("NeId");
        cols.add("LocateNeName");
        cols.add("LocateNeType");
        cols.add("NeName");
        cols.add("NeAlias");
        cols.add("EquipmentClass");
        cols.add("NeIp");
        cols.add("SystemName");
        cols.add("Vendor");
        cols.add("Version");
        cols.add("LocateNeStatus");
        cols.add("ProjectNo");
        cols.add("ProjectName");
        cols.add("ProjectUserNum");
        cols.add("ProjectStartTime");
        cols.add("ProjectEndTime");
        cols.add("LocateInfo");
        cols.add("EventTime");
        cols.add("CancelTime");
        cols.add("DalTime");
        cols.add("VendorAlarmType");
        cols.add("VendorSeverity");
        cols.add("AlarmSeverity");
        cols.add("VendorAlarmId");
        cols.add("NmsAlarmId");
        cols.add("AlarmStatus");
        cols.add("AckFlag");
        cols.add("AckTime");
        cols.add("AckUser");
        cols.add("AlarmTitle");
        cols.add("StandardAlarmName");
        cols.add("ProbableCauseTxt");
        cols.add("AlarmText");
        cols.add("CircuitNo");
        cols.add("PortRate");
        cols.add("Specialty");
        cols.add("BusinessSystem");
        cols.add("AlarmLogicClass");
        cols.add("AlarmLogicSubClass");
        cols.add("EffectOnEquipment");
        cols.add("EffectOnBusiness");
        cols.add("NmsAlarmType");
        cols.add("SendGroupFlag");
        cols.add("RelatedFlag");
        cols.add("AlarmProvince");
        cols.add("AlarmRegion");
        cols.add("AlarmCounty");
        cols.add("Site");
        cols.add("AlarmActCount");
        cols.add("CorrelateAlarmFlag");
        cols.add("SheetSendStatus");
        cols.add("SheetStatus");
        cols.add("SheetNo");
        cols.add("AlarmMemo");
        cols.add("dt_event_day");
        cols.add("dt_event_week");
        cols.add("dt_day");
        cols.add("dt_month");
        cols.add("dt_hour");
        return cols;
    }

    public static Integer getWeek(String date){
        LocalDate localDate = LocalDate.parse(date, TIME_FORMATTER);
        WeekFields weekFields=WeekFields.ISO;
        int i = localDate.get(weekFields.weekOfYear());
        AtomicInteger atomicInteger=new AtomicInteger();
        atomicInteger.set(i);
        int j = atomicInteger.addAndGet(1);
//        System.out.println(i+1+"---------i");
//        System.out.println(j+"***---------i");
//        int b = localDate.get(weekFields.weekBasedYear());
//        System.out.println(b+"---------i");
//        int c = localDate.get(weekFields.weekOfWeekBasedYear());
//        System.out.println(c+"---------i");

        return j;
    }

    public static int getProjectUserNum(String projectName){
//        String s="影响ONU数量:98 $C类";
        if (isEmpty(projectName)){
            return -1;
        }
        String s1 = projectName.split(":")[1];
        String s2 = s1.split("\\$")[0].trim();
        return Integer.parseInt(s2);
    }

    public static List<String> getWirelessList() {
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
        cols.add("dt_hour");
        cols.add("dt_event_day");
        return cols;
    }

    //校验json
    public static boolean isJsonValidate(String log) {
        try {
            JSON.parse(log);
            return true;
        } catch (JSONException e) {
            return false;
        }
    }
    //判断空值
    public static boolean isEmpty(Object object) {
        if (object == null) {
            return (true);
        }
        if ("".equals(object)) {
            return (true);
        }
        if ("null".equals(object)) {
            return (true);
        }
        return (false);
    }

    public static boolean isNotEmpty(Object object) {
        return !isEmpty(object);
    }

    /**
     * 判断 list 是否为空
     *
     * @param list
     * @return true or false
     * list == null		: true
     * list.size() == 0	: true
     */
    public static boolean listIsEmpty(Collection list) {
        return (list == null || list.size() == 0);
    }

    /**
     * 判断 list 是否不为空
     *
     * @param list
     * @return true or false
     * list == null		: false
     * list.size() == 0	: false
     */
    public static boolean listIsNotEmpty(Collection list) {
        return !listIsEmpty(list);
    }
}