package com.alone.kafka.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import static com.alone.kafka.test.ReadFiles.dateToLocalDate;

public class DateUtils {
    //    public static void main(String[] args) {
//        LocalDate localDateTime=LocalDate.now();
//        DateTimeFormatter dateTimeFormatter=DateTimeFormatter.ofPattern("yyyy-MM-dd");
//        DateTimeFormatter dateTimeFormatter1=DateTimeFormatter.ofPattern("yyyy-MM");
//        String format = dateTimeFormatter.format(localDateTime);
//        String format1 = dateTimeFormatter1.format(localDateTime);
//        System.out.println(format);
//        System.out.println(format1);
//    }
    private final static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private final static DateTimeFormatter MONTH_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM");
    private final static DateTimeFormatter HOUR_FORMATTER = DateTimeFormatter.ofPattern("HH");
    private final static DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static void main(String[] args) {
//        String d="2021-08-11 15:33:44";
//        String c="\"ss\"";
//        LocalDateTime parse = LocalDateTime.parse(d, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
//        System.out.println(parse);
//        boolean b = c.startsWith("\"");
//        System.out.println(b);
//        LocalDateTime dateTime = LocalDateTime.now();
//        LocalDateTime dateTime = LocalDateTime.of(2021,01,20,10,30,40);
//        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("HH");
//        System.out.println(formatter.format(dateTime));

//        LocalDateTime now =LocalDateTime.now();
//        System.out.println(DATE_TIME_FORMATTER.format(now));
//        System.out.println(MONTH_FORMATTER.format(now));
//        System.out.println(HOUR_FORMATTER.format(now));

//        System.out.println("    ");
//        System.out.println("\t");
//        String s="EventTime:2021-09-13 15:03:24";
//        String s="<AlarmStart> LocateInfo:设备172.1.51.166(编码;类型OLT;型号C600)告警: 1. 光纤链路故障；2. ONU故障。(DESCR=:GPON alarm link olt losi(lobi) (rack 1 shelf 1 slot 1 port 3 onu 14)) <AlarmEnd>";
//        String[] split = s.split(":", 2);
//        for (String a:split
//             ) {
//            System.out.println(a);
//        }
//        if (!(s.startsWith("<AlarmStart>")&&s.endsWith("<AlarmEnd>"))){
//            System.out.println(s);
//        }else {
//            System.out.println("000000");
//        }
        LocalDateTime dateTime = LocalDateTime.parse("2021-09-22 18:26:07",TIME_FORMATTER);
        System.out.println(dateTime);
        System.out.println(DATE_FORMATTER.format(dateTime));
    }
}
