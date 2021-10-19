package com.alone.kafka.consumer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.WeekFields;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import com.alone.kafka.entry.Offset;
import com.alone.kafka.utils.DBUtils;
import com.alone.kafka.utils.MapUtils;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import static com.alone.kafka.consumer.KafkaBatchConsumer.isEmpty;
import static com.alone.kafka.consumer.KafkaBatchConsumer.isNotEmpty;
import static com.alone.kafka.test.ReadFiles.dateToLocalDate;
import static com.alone.kafka.test.ReadFiles.getObjectToMap;

/**
 * @author Administrator
 */
public class KafkaBatchConsumerOlt {


    private static Properties properties = null;
    /**
     * 正式
     */
    private final static String TOPIC = "province-share-heb-banms-asiainfo";
    private final static String GROUP = "kafka-dop-group-olt";
    /**
     * 测试
     */
//    private static String GROUP = "test_second_group";
//    private static String TOPIC = "test_second";

    //**********************offset 保存数据表名 update 20211019
    private final static String TABLE="offset_management";
//    private final static String TABLE="offset";
    //**********************offset 保存数据表名 update 20211019

    private static KafkaConsumer<String, String> consumer;
    private static String ip;
    private static String MAX_POLL;
    private final static DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private final static DateTimeFormatter MONTH_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM");
    private final static DateTimeFormatter HOUR_FORMATTER = DateTimeFormatter.ofPattern("HH");
    private final static DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    static {
        try {
            Properties prop = new Properties();
            // 加载配置文件, 调用load()方法
            // 类加载器加载资源时, 去固定的类路径下查找资源, 因此, 资源文件必须放到src目录才行
            prop.load(KafkaBatchConsumerOlt.class.getClassLoader().getResourceAsStream("kafka.properties"));
            // 从配置文件中获取数据为成员变量赋值
            ip = prop.getProperty("kafka.bootstrap-servers").trim();
            MAX_POLL = prop.getProperty("kafka.MAX_POLL").trim();
        } catch (IOException e) {
            e.printStackTrace();
        }

        properties = new Properties();
        // kafka集群，broker-list
//            properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.6.42:9092,192.168.6.43:9092,192.168.6.44:9092");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ip);

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // 消费者组，只要group.id相同，就属于同一个消费者组
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP);
        // 关闭自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//            properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL);

        // 1.创建一个消费者
        consumer = new KafkaConsumer<String, String>(properties);
    }


    @SneakyThrows
    public static void main(String[] args) {

        consumer.subscribe(Collections.singletonList(TOPIC), new ConsumerRebalanceListener() {

            // rebalance之前将记录进行保存
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                consumer.commitSync();
                for (TopicPartition partition : partitions) {
                    // 获取分区
                    int subTopicPartitionId = partition.partition();
                    // 对应分区的偏移量
                    long subTopicPartitionOffset = consumer.position(partition);
                    System.out.println("onPartitionsRevoked" + subTopicPartitionOffset);
                    String date = new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(
                            new Date(System.currentTimeMillis())
                    );
                    DBUtils.update("replace into "+TABLE+" values(?,?,?,?,?)",
                            new Offset(
                                    GROUP,
                                    TOPIC,
                                    subTopicPartitionId,
                                    subTopicPartitionOffset,
                                    date
                            )
                    );
                }
                //                    consumer.commitAsync();
                System.out.println("日期：" + new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(
                        new Date(System.currentTimeMillis())
                ) + "  onPartitionsRevoked触发了");
            }

            // rebalance之后读取之前的消费记录，继续消费
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    int subtopicpartitionid = partition.partition();
                    long offset = DBUtils.queryOffset(
                            "select sub_topic_partition_offset from "+TABLE+" where consumer_group=? and sub_topic=? and sub_topic_partition_id=?",
                            //                            "select untiloffset from offset_manager where groupid=? and topic=? and partition=?",
                            GROUP,
                            TOPIC,
                            subtopicpartitionid
                    );
                    System.out.println("partition = " + partition + "--------offset = " + offset);
                    // 定位到最近提交的offset位置继续消费
                    if (offset == 0) {
                        consumer.seek(partition, offset);
                    } else {
                        consumer.seek(partition, offset + 1);
                    }
                }
                System.out.println("日期：" + new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(
                        new Date(System.currentTimeMillis())
                ) + "  onPartitionsAssigned触发了");
            }
        });
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(5000);
                if (records.isEmpty()) {
                    continue;
                }
                //******************************************日志
//                System.out.println(records.count() + "条----------时间：" + new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(
//                        new Date(System.currentTimeMillis())
//                ));
                //******************************************日志
                //            List<AlarmMessage> list = new ArrayList<>();
                List<Map<String, Object>> dataList = new ArrayList<>();
                //            List<Map<String, Object>> data = new ArrayList<>();
                //            System.getProperty()
                List<Offset> offsets = new ArrayList<Offset>();
                for (ConsumerRecord<String, String> record : records) {
                    String date = new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(
                            new Date(System.currentTimeMillis())
                    );
                    offsets.add(new Offset(GROUP, TOPIC, record.partition(), record.offset(), date));
                    //******************************************日志
//                    System.out.println("|---------------------------------------------------------------\n" +
//                            "|group\ttopic\tpartition\toffset\ttimestamp\n" +
//                            "|" + GROUP + "\t" + TOPIC + "\t" + record.partition() + "\t" + record.offset() + "\t" + new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(record.timestamp()) + "\n" +
//                            "|---------------------------------------------------------------"
//                    );
//                    System.out.println(record.value());
                    //******************************************日志
                    Optional<?> kafkaMessage = Optional.ofNullable(record.value());
                    if (kafkaMessage.isPresent()) {
                        Object message = record.value();
//                        String topic = record.topic();
//                        long offset = record.offset();
//                        String date = new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(
//                                new Date(System.currentTimeMillis())
//                        );
//                        System.out.println("offset:=========================================" + offset);
//                        System.out.println("接收到消息：" + message);
//                        System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
                        //过滤message为空值的情况
                        if (isEmpty(message)) {
                            System.out.println("????????????????");
                            System.out.println("|---------------------------------------------------------------\n" +
                                    "|group\ttopic\tpartition\toffset\ttimestamp\n" +
                                    "|" + GROUP + "\t" + TOPIC + "\t" + record.partition() + "\t" + record.offset() + "\t" + new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(record.timestamp()) + "\n" +
                                    "|---------------------------------------------------------------"
                            );
                            System.out.println("接收到的消息有空值！：" + message + "前面是空值");
                            System.out.println("????????????????");
                            continue;
                        }
                        String data = message.toString();
                        //                    if (!(data.startsWith("<AlarmStart>")&&data.endsWith("<AlarmEnd>"))) {
                        if (!(data.contains("<AlarmStart>") && data.contains("<AlarmEnd>"))) {
                            System.out.println("^^^^^^^^^^^^^^^^^^^^^^^");
                            System.out.println("|---------------------------------------------------------------\n" +
                                    "|group\ttopic\tpartition\toffset\ttimestamp\n" +
                                    "|" + GROUP + "\t" + TOPIC + "\t" + record.partition() + "\t" + record.offset() + "\t" + TIME_FORMATTER.format(LocalDateTime.now()) + "\n" +
                                    "|---------------------------------------------------------------"
                            );
                            System.out.println("接收到的消息不标准！：" + data);
                            System.out.println("^^^^^^^^^^^^^^^^^^^^^^^");
                            continue;
                        }
                        String s = data.replaceAll("<AlarmStart>", "").replaceAll("<AlarmEnd>", "");
                        //                System.out.println("new====="+s);
                        String[] a = s.split("\\n    ");
                        Map<String, Object> map = new HashMap<>();
                        for (String ss : a) {
                            String s1 = ss.replaceAll("\\n", "");
                            if (isEmpty(s1)) {
                                continue;
                            }
                            String[] split = s1.split(":", 2);
                            //    System.out.println(split[0]);
                            if (split.length < 2) {
                                //  System.out.println(split.length);
                                //  map.put(split[0], "" + "offset" + record.offset());
                                map.put(split[0], "");
                                continue;
                            }
                            map.put(split[0], split[1]);
                            //********************************update 20211012 去除NmsAlarmId，非310003，310004，310006
//                            if("NmsAlarmId".equalsIgnoreCase(split[0])&&"".equalsIgnoreCase(split[1])){
//                                break;
//                            }
                            //********************************update 20211012 去除NmsAlarmId，非310003，310004，310006
                            // System.out.println(split.length);
                            // map.put(split[0], split[1] + "offset" + record.offset());
                            if (isNotEmpty(map.get("EventTime"))) {
                                //获取数据中的EventTime字段，并转换为LocalDateTime
                                String eventTime = (String) map.get("EventTime");
                                LocalDateTime dateTime = LocalDateTime.parse(eventTime, TIME_FORMATTER);
                                //*******************update 20211011 新增告警发生时间-保存到日
                                map.put("dt_event_day", DATE_FORMATTER.format(dateTime));
                                //*******************update 20211011 新增告警发生时间-保存到日
                                //*******************update 20211014 新增告警发生时间-保存当年多少周
                                map.put("dt_event_week",getWeek(eventTime));
                                //*******************update 20211014 新增告警发生时间-保存当年多少周
                            }
                        }
                        //********************update20211019 ProjectName 切割字符串，获取用户数量 ProjectUserNum
                        String projectName = (String) map.get("ProjectName");
                        map.put("ProjectUserNum",getProjectUserNum(projectName));
                        //********************update20211019 ProjectName 切割字符串，获取用户数量 ProjectUserNum
                        String nmsAlarmId = (String) map.get("NmsAlarmId");
                        String alarmStatus = (String) map.get("AlarmStatus");
                        if ("".equalsIgnoreCase(nmsAlarmId) && "1".equalsIgnoreCase(alarmStatus)) {
//                            System.out.println("有问题！");
                            continue;
                        } else {
                            //手动记录日期
                            LocalDateTime now = LocalDateTime.now();
                            map.put("dt_day", DATE_FORMATTER.format(now));
                            map.put("dt_month", MONTH_FORMATTER.format(now));
                            map.put("dt_hour", HOUR_FORMATTER.format(Objects.requireNonNull(now)));
                            dataList.add(map);
                        }
                        //***************************************日志
//                        System.out.println("-------------------------------------------------------------------------接受到的数据start");
//                        System.out.println(map);
//                        System.out.println("-------------------------------------------------------------------------接受到的数据end");
                        //***************************************日志
                        //                    OltMessage oltMessage = (OltMessage) MapUtils.mapToObject(map, OltMessage.class);
                        //                    Map<String, Object> toMap = getObjectToMap(oltMessage);
                        //                    data.add(toMap);
                        //                System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
                        //                System.out.println(alarmMessage);
                    }

                }
//                System.out.println(dataList);
                //获取数据库入参字段
                List<String> cols = getList();
                //******************************update
                DBUtils.insertAllByList("ods_iscs_olt_alarm", dataList, cols);
                //******************************update
                for (Offset offset : offsets) {
                    DBUtils.update("replace into "+TABLE+" values(?,?,?,?,?)", offset);
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                offsets.clear();
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.out.println("有问题的时间："+TIME_FORMATTER.format(LocalDateTime.now()));
            e.printStackTrace();
        }
    }

    public static List<String> getList() {
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
}



