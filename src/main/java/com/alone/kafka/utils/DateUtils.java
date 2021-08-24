package com.alone.kafka.utils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

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
    public static void main(String[] args) {
        String d="2021-08-11 15:33:44";
        String c="\"ss\"";
        LocalDateTime parse = LocalDateTime.parse(d, DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        System.out.println(parse);
        boolean b = c.startsWith("\"");
        System.out.println(b);
    }
}
