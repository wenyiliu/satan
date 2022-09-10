package com.satan.common.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author liuwenyi
 * @date 2022/8/23
 **/
public class ReceiveLatestMessageMain {
    private static final int COUNT = 100;

    public static void main(String... args) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.230.0.8:6667");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "yq-consumer12");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        System.out.println("create KafkaConsumer");
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        AdminClient adminClient = AdminClient.create(props);
        String topic = "bitmart_beta_fvirtualcaptualoperation";
        adminClient.describeTopics(Collections.singletonList(topic));
        try {
            DescribeTopicsResult topicResult = adminClient.describeTopics(Collections.singletonList(topic));
            Map<String, KafkaFuture<TopicDescription>> descMap = topicResult.values();
            for (Map.Entry<String, KafkaFuture<TopicDescription>> entry : descMap.entrySet()) {
                System.out.println("key: " + entry.getKey());
                List<TopicPartitionInfo> topicPartitionInfoList = entry.getValue().get().partitions();
                topicPartitionInfoList.forEach((e) -> {
                    int partitionId = e.partition();
                    Node node = e.leader();
                    TopicPartition topicPartition = new TopicPartition(topic, partitionId);
                    Map<TopicPartition, Long> mapBeginning = consumer.beginningOffsets(Collections.singletonList(topicPartition));
                    Iterator<Map.Entry<TopicPartition, Long>> itr2 = mapBeginning.entrySet().iterator();
                    long beginOffset = 0;
                    //mapBeginning只有一个元素，因为Arrays.asList(topicPartition)只有一个topicPartition
                    while (itr2.hasNext()) {
                        Map.Entry<TopicPartition, Long> tmpEntry = itr2.next();
                        beginOffset = tmpEntry.getValue();
                    }
                    Map<TopicPartition, Long> mapEnd = consumer.endOffsets(Collections.singletonList(topicPartition));
                    Iterator<Map.Entry<TopicPartition, Long>> itr3 = mapEnd.entrySet().iterator();
                    long lastOffset = 0;
                    while (itr3.hasNext()) {
                        Map.Entry<TopicPartition, Long> tmpEntry2 = itr3.next();
                        lastOffset = tmpEntry2.getValue();
                    }
                    long expectedOffSet = lastOffset - COUNT;
                    expectedOffSet = expectedOffSet > 0 ? expectedOffSet : 1;
                    System.out.println("Leader of partitionId: " + partitionId + "  is " + node + ".  expectedOffSet:" + expectedOffSet
                            + "，  beginOffset:" + beginOffset + ", lastOffset:" + lastOffset);
                    consumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(expectedOffSet - 1)));
                });
            }
//            kafka-topics.sh --list --zookeeper 10.230.0.8:6667


            consumer.subscribe(Collections.singletonList(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(101);
                for (ConsumerRecord<String, String> record : records) {
                    String value = record.value();
                    System.out.printf("read offset =%d, key=%s , value= %s, partition=%s\n",
                            record.offset(), record.key(), value, record.partition());

                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("when calling kafka output error." + ex.getMessage());
        } finally {
            adminClient.close();
            consumer.close();
        }
    }
}
