package com.alone.kafka.consumer;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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

import static com.alone.kafka.utils.oConvertUtils.*;

/**
 * @author Administrator
 */
public class KafkaBatchConsumer {


    private static Properties properties = null;

    /**
     * 线上topic和group
     */
    private final static String GROUP = "kafka-dop-group-first";
    private final static String TOPIC = "province-share-heb-wuxian234g-dop";
    private final static String OFFSET_TABLE = "offset_management";
    //测试
//    private static String GROUP = "test_second_group";
//    private static String TOPIC = "test_second";
//    private final static String OFFSET_TABLE = "offset";

    private static KafkaConsumer<String, String> consumer;
    private static String ip;
    private static String MAX_POLL;
    private final static DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private final static DateTimeFormatter MONTH_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM");
    private final static DateTimeFormatter HOUR_FORMATTER = DateTimeFormatter.ofPattern("HH");
    private final static DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private final static DateTimeFormatter YEAR_FORMATTER = DateTimeFormatter.ofPattern("yyyy");

    static {
        try {
            Properties prop = new Properties();
            // 加载配置文件, 调用load()方法
            // 类加载器加载资源时, 去固定的类路径下查找资源, 因此, 资源文件必须放到src目录才行
            prop.load(KafkaBatchConsumer.class.getClassLoader().getResourceAsStream("kafka.properties"));
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
                    DBUtils.update("replace into " + OFFSET_TABLE + " values(?,?,?,?,?)",
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
                            "select sub_topic_partition_offset from " + OFFSET_TABLE + " where consumer_group=? and sub_topic=? and sub_topic_partition_id=?",
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
                    //*******************update offset存放方式
                    int flag=0;
                    if (listIsEmpty(offsets)){
                        offsets.add(new Offset(GROUP, TOPIC, record.partition(), record.offset(), date));
                    }else {
                        for (Offset o : offsets) {
                            if (isEmpty(o)) {
                                continue;
                            }
                            if (record.partition()==o.getSubTopicPartitionId()){
                                o.setSubTopicPartitionOffset(record.offset());
                                o.setTimestamp(date);
                                flag=-1;
                                break;
                            }
                        }
                        if (flag==0){
                            offsets.add(new Offset(GROUP, TOPIC, record.partition(), record.offset(), date));
                        }
                    }
                    //*******************update offset存放方式
                    //这里有问题，上面已优化，下面弃用
//                    offsets.add(new Offset(GROUP, TOPIC, record.partition(), record.offset(), date));
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
                        //过滤特殊格式数据
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
                        //过滤非json格式
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
                        //过滤发生时间为空
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
                        //过滤时间格式
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
                        //过滤非enodeB和Eutrancell
//                    if (!("eNodeB".equalsIgnoreCase(alarmMessage.getLocateNeType())
//                            ||"Eutrancell".equalsIgnoreCase(alarmMessage.getLocateNeType()))){
//                        System.out.println("```````````````````````````````````````");
//                        System.out.println(alarmMessage);
//                        System.out.println("|---------------------------------------------------------------\n" +
//                                "|group\ttopic\tpartition\toffset\ttimestamp\n" +
//                                "|" + GROUP + "\t" + TOPIC + "\t" + record.partition() + "\t" + record.offset() + "\t" + new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(record.timestamp()) + "\n" +
//                                "|---------------------------------------------------------------"
//                        );
//                        System.out.println("过滤非enodeB和Eutrancell！");
//                        System.out.println(record.value());
//                        System.out.println("```````````````````````````````````````");
//                        continue;
//                    }
                        LocalDateTime now = LocalDateTime.now();
                        alarmMessage.setDt_month(MONTH_FORMATTER.format(Objects.requireNonNull(dateTime)));
                        alarmMessage.setDt_day(DATE_TIME_FORMATTER.format(Objects.requireNonNull(now)));
                        //***************update 新增小时字段入库 20210901
                        //***************update 新增小时字段入库 20211027 修改为eventTime
                        alarmMessage.setDt_hour(HOUR_FORMATTER.format(dateTime));
//                    System.out.println(alarmMessage);
//                    System.out.println(alarmMessage.getDt_hour());
                        //***************update 新增小时字段入库
                        //**********************update 20211010 新增 保存eventTime的年月日
                        alarmMessage.setDt_event_day(DATE_TIME_FORMATTER.format(dateTime));
                        //**********************update 新增 保存eventTime的年月日
                        //*********************update 20211025 新增week字段
                        alarmMessage.setDt_event_week(getWeek(TIME_FORMATTER.format(dateTime)));
                        //************************update 20211108 新增年字段，配合周使用
                        alarmMessage.setDt_event_year(YEAR_FORMATTER.format(dateTime));
                        //************************update 20211108 新增年字段，配合周使用
//                        System.out.println(alarmMessage);
                        //*********************update 20211025 新增week
                        Map<String, Object> map = getObjectToMap(alarmMessage);
                        dataList.add(map);
                        //*********************update 20210817 获取kafka消费数据json格式，转换入库
                    }

                }
                List<String> cols = getWirelessList();
                //******************************update
                DBUtils.insertAllByList("ods_iscs_wireless_alarm", dataList, cols);
                //******************************update
                //********************************************日志
//            System.out.println("^^^^&&&&&&");
//            System.out.println(offsets);
//            System.out.println("^^^^&&&&&&");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                //********************************************日志
                //************************************************update db
                DBUtils.updateList("replace into "+OFFSET_TABLE+" values(?,?,?,?,?)",offsets);
                //*******************************************************update db
                offsets.clear();
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            System.out.println("有问题的时间：" + TIME_FORMATTER.format(LocalDateTime.now()));
            e.printStackTrace();
        }
    }
}



