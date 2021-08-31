package com.alone.kafka.consumer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.alone.kafka.entry.AlarmMessage;
import com.alone.kafka.entry.Offset;
import com.alone.kafka.utils.DBUtils;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import static com.alone.kafka.test.ReadFiles.*;

/**
 * @author Administrator
 */
public class KafkaBatchConsumer {


    private static Properties properties = null;
    // private static String group = "mysql_offset";
//    private static String group = "alarm_test";
    private final static String GROUP = "kafka-dop-group-first";
    //    private static String topic = "first";
    private final static String TOPIC = "province-share-heb-wuxian234g-dop";
    //    private static String topic = "test_second";
//    private static String topic = "edgenode_student4";
    private static KafkaConsumer<String, String> consumer;
    private static String ip;
    private static String MAX_POLL;
    private final static DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private final static DateTimeFormatter MONTH_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM");

    static {
        try {
            Properties prop = new Properties();
            // 加载配置文件, 调用load()方法
            // 类加载器加载资源时, 去固定的类路径下查找资源, 因此, 资源文件必须放到src目录才行
            prop.load(DBUtils.class.getClassLoader().getResourceAsStream("kafka.properties"));
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
//        System.out.println(System.getProperty("java.version"));
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
                    DBUtils.update("replace into offset values(?,?,?,?,?)",
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
                System.out.println("onPartitionsRevoked触发了");
            }

            // rebalance之后读取之前的消费记录，继续消费
            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                for (TopicPartition partition : partitions) {
                    int subtopicpartitionid = partition.partition();
                    long offset = DBUtils.queryOffset(
                            "select sub_topic_partition_offset from offset where consumer_group=? and sub_topic=? and sub_topic_partition_id=?",
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
                System.out.println("onPartitionsAssigned触发了");
            }
        });

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(5000);
            if (records.isEmpty()) {
                continue;
            }
            //*******************************日志
//            System.out.println(records.count() + "条----------时间：" + new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(
//                    new Date(System.currentTimeMillis())
//            ));
            //*******************************日志
            List<Map<String, Object>> dataList = new ArrayList<>();
            List<Offset> offsets = new ArrayList<Offset>();
            for (ConsumerRecord<String, String> record : records) {
                String date = new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(
                        new Date(System.currentTimeMillis())
                );
                offsets.add(new Offset(GROUP, TOPIC, record.partition(), record.offset(), date));
                //*******************************日志
//                System.out.println("|---------------------------------------------------------------\n" +
//                        "|group\ttopic\tpartition\toffset\ttimestamp\n" +
//                        "|" + GROUP + "\t" + TOPIC + "\t" + record.partition() + "\t" + record.offset() + "\t" + new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(record.timestamp()) + "\n" +
//                        "|---------------------------------------------------------------"
//                );
//                System.out.println(record.value());
                //*******************************日志
                Optional<?> kafkaMessage = Optional.ofNullable(record.value());
                if (kafkaMessage.isPresent()) {
                    Object message = record.value();
                    String topic = record.topic();
                    long offset = record.offset();
//                String date = new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(
//                        new Date(System.currentTimeMillis())
//                );
                    //*******************************日志
//                    System.out.println("offset:=========================================" + offset);
//                    System.out.println("接收到消息：" + message);
//                    System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
                    //*******************************日志
                    //*********************update 20210817 获取kafka消费数据json格式，转换入库
                    //过滤message为空值的情况
                    if (isEmpty(message)){
                        System.out.println("????????????????");
                        System.out.println("|---------------------------------------------------------------\n" +
                                "|group\ttopic\tpartition\toffset\ttimestamp\n" +
                                "|" + GROUP + "\t" + TOPIC + "\t" + record.partition() + "\t" + record.offset() + "\t" + new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(record.timestamp()) + "\n" +
                                "|---------------------------------------------------------------"
                        );
                        System.out.println("接收到的消息有空值！："+message+"前面是空值");
                        System.out.println("????????????????");
                        continue;
                    }
                    String data = message.toString();
                    if (data.startsWith("\"") || data.endsWith("\"")) {
                        System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$");
                        System.out.println("|---------------------------------------------------------------\n" +
                                "|group\ttopic\tpartition\toffset\ttimestamp\n" +
                                "|" + GROUP + "\t" + TOPIC + "\t" + record.partition() + "\t" + record.offset() + "\t" + new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(record.timestamp()) + "\n" +
                                "|---------------------------------------------------------------"
                        );
                        System.out.println("前或后有引号！");
                        System.out.println(record.value());
                        System.out.println("$$$$$$$$$$$$$$$$$$$$$$$$$");
                        continue;
                    }
                    if (!isJsonValidate(data)) {
                        System.out.println("<<<<<<<>>>>>>");
                        System.out.println("|---------------------------------------------------------------\n" +
                                "|group\ttopic\tpartition\toffset\ttimestamp\n" +
                                "|" + GROUP + "\t" + TOPIC + "\t" + record.partition() + "\t" + record.offset() + "\t" + new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(record.timestamp()) + "\n" +
                                "|---------------------------------------------------------------"
                        );
                        System.out.println("格式不正确！");
                        System.out.println(record.value());
                        System.out.println("<<<<<<<>>>>>>");
                        continue;
                    }
                    AlarmMessage alarmMessage = null;
                    try {
                        alarmMessage = JSONObject.parseObject(data, AlarmMessage.class);
                        //*******************************日志
//                        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!");
//                        System.out.println("接收到的实体对象是："+alarmMessage);
//                        System.out.println("!!!!!!!!!!!!!!!!!!!!!!!!!");
                        //*******************************日志
                    } catch (Exception e) {
                        e.printStackTrace();
                        System.out.println("==========================");
                        System.out.println("|---------------------------------------------------------------\n" +
                                "|group\ttopic\tpartition\toffset\ttimestamp\n" +
                                "|" + GROUP + "\t" + TOPIC + "\t" + record.partition() + "\t" + record.offset() + "\t" + new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(record.timestamp()) + "\n" +
                                "|---------------------------------------------------------------"
                        );
                        System.out.println("字段不正确！");
                        System.out.println(record.value());
                        System.out.println("==========================");
                        continue;
                    }
                    if (null == (alarmMessage.getEventTime())) {
                        System.out.println("@@@@@@@@@@@@@@@@@@@");
                        System.out.println("|---------------------------------------------------------------\n" +
                                "|group\ttopic\tpartition\toffset\ttimestamp\n" +
                                "|" + GROUP + "\t" + TOPIC + "\t" + record.partition() + "\t" + record.offset() + "\t" + new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(record.timestamp()) + "\n" +
                                "|---------------------------------------------------------------"
                        );
                        System.out.println("时间为空不正确！");
                        System.out.println(record.value());
                        System.out.println("@@@@@@@@@@@@@@@@@@@");
                        continue;
                    }
                    LocalDateTime dateTime = dateToLocalDate(alarmMessage.getEventTime());
                    if (null == dateTime) {
                        System.out.println("////////////////////////");
                        System.out.println("|---------------------------------------------------------------\n" +
                                "|group\ttopic\tpartition\toffset\ttimestamp\n" +
                                "|" + GROUP + "\t" + TOPIC + "\t" + record.partition() + "\t" + record.offset() + "\t" + new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(record.timestamp()) + "\n" +
                                "|---------------------------------------------------------------"
                        );
                        System.out.println("时间格式不正确！");
                        System.out.println(record.value());
                        System.out.println("////////////////////////");
                        continue;
                    }
                    alarmMessage.setDt_month(MONTH_FORMATTER.format(Objects.requireNonNull(dateTime)));
                    alarmMessage.setDt_day(DATE_TIME_FORMATTER.format(Objects.requireNonNull(dateTime)));
                    Map<String, Object> map = getObjectToMap(alarmMessage);
                    dataList.add(map);
                    //*********************update 20210817 获取kafka消费数据json格式，转换入库
                }

            }
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
            //******************************update
//            DBUtils.insertAllByList("alarm_message_tmp", dataList, cols);
            DBUtils.insertAllByList("ods_iscs_wireless_alarm", dataList, cols);
            //******************************update
            for (Offset offset : offsets) {
                DBUtils.update("replace into offset values(?,?,?,?,?)", offset);
            }
            offsets.clear();
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
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
}



