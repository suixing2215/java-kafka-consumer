package com.alone.kafka.entry;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Administrator
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Offset {
    private String consumerGroup;
    private String subTopic;
    private Integer subTopicPartitionId;
    private Long subTopicPartitionOffset;
    private String timestamp;
}
