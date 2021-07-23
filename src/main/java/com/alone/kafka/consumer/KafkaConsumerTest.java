package com.alone.kafka.consumer;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import com.alone.kafka.entry.Offset;
import com.alone.kafka.utils.DBUtils;
import lombok.SneakyThrows;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * @author Administrator
 */
public class KafkaConsumerTest {


    private static Properties properties = null;
    // private static String group = "mysql_offset";
//    private static String group = "alarm_test";
    private static String group = "test_second_group";
//    private static String topic = "first";
    private static String topic = "test_second";
    private static KafkaConsumer<String, String> consumer;
    private static String ip;
    private static String MAX_POLL;

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
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        // 关闭自动提交offset
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
//            properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);
        properties.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL);

        // 1.创建一个消费者
        consumer = new KafkaConsumer<String, String>(properties);
    }


    @SneakyThrows
    public static void main(String[] args) {
//        System.out.println(System.getProperty("java.version"));q
        consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {

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
                                    group,
                                    topic,
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
                            group,
                            topic,
                            subtopicpartitionid
                    );
                    System.out.println("partition = " + partition + "--------offset = " + offset);
                    // 定位到最近提交的offset位置继续消费
                    if (offset == 0) {
                        consumer.seek(partition, offset);
                    } else {
                        consumer.seek(partition, offset+1);
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
            System.out.println(records.count() + "条----------时间：" + new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(
                    new Date(System.currentTimeMillis())
            ));
//            List<AlarmMessage> list = new ArrayList<>();
            List<Map<String, Object>> dataList = new ArrayList<>();
//            System.getProperty()
            List<Offset> offsets = new ArrayList<Offset>();
            for (ConsumerRecord<String, String> record : records) {
                String date = new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(
                        new Date(System.currentTimeMillis())
                );
                offsets.add(new Offset(group, topic, record.partition(), record.offset(), date));

                System.out.println("|---------------------------------------------------------------\n" +
                        "|group\ttopic\tpartition\toffset\ttimestamp\n" +
                        "|" + group + "\t" + topic + "\t" + record.partition() + "\t" + record.offset() + "\t" + record.timestamp() + "\n" +
                        "|---------------------------------------------------------------"
                );
                System.out.println(record.value());
                Optional<?> kafkaMessage = Optional.ofNullable(record.value());
                if (kafkaMessage.isPresent()) {
                    Object message = record.value();
                    String topic = record.topic();
                    long offset = record.offset();
//                String date = new SimpleDateFormat("yyyy年MM月dd日 HH:mm:ss").format(
//                        new Date(System.currentTimeMillis())
//                );
//                offsetMapper.update("test-group", topic, record.partition(), offset, date);
                    System.out.println("offset:=========================================" + offset);
                    System.out.println("接收到消息：" + message);
                    System.out.println("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^");
                    String s = message.toString().replaceAll("<AlarmStart>", "").replaceAll("<AlarmEnd>", "");
//                System.out.println("new====="+s);
                    String[] a = s.split("\\n\\t");
                    Map<String, Object> map = new HashMap<>();
                    for (String ss : a) {
//                    System.out.println("--------------");``
//                    System.out.println(ss);
                        String s1 = ss.replaceAll("\\n", "");
//                    System.out.println("+++++++++++++++++++++++");
//                    System.out.println(s1);
                        String[] split = s1.split(":");
                        if (split.length < 2) {
//                        System.out.println(split.length);
                            map.put(split[0], "" + "offset" + record.offset());
                            continue;
                        }
//                    System.out.println(split.length);
                        map.put(split[0], split[1] + "offset" + record.offset());
                    }
                    dataList.add(map);
//                    AlarmMessage alarmMessage = (AlarmMessage) MapUtils.mapToObject(map, AlarmMessage.class);
//                    list.add(alarmMessage);
//                System.out.println("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&");
//                System.out.println(alarmMessage);
                }

            }
            List<String> cols = new ArrayList<>();
            cols.add("IntVersion");
            cols.add("MsgSerial");
            cols.add("AlarmUniqueId");
            cols.add("ClearId");
            cols.add("NeId");
            DBUtils.insertAllByList("alarm_message", dataList, cols);
            for (Offset offset : offsets) {
                DBUtils.update("replace into offset values(?,?,?,?,?)", offset);
            }
            offsets.clear();
//                consumer.close();
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}



