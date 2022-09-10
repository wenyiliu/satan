package com.satan.common;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @author liuwenyi
 * @date 2022/6/28
 **/
public class Consumer {
    public static void main(String[] args) {
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", "10.230.0.8:6667,10.230.0.9:6667,10.230.0.10:6667");
        consumerProperties.setProperty("group.id", "BinlogDelayAlarm");
        consumerProperties.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        consumerProperties.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);
        consumer.subscribe(Collections.singletonList("flinkcdc_trade3"));
        while (true) {
            /*
             * poll() API 是拉取消息的长轮询
             */
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("收到消息：partition = %d,offset = %d, key = %s, value = %s%n", record.partition(),
                        record.offset(), record.key(), record.value());
            }

            /*if (records.count() > 0) {
                // 手动同步提交offset，当前线程会阻塞直到offset提交成功
                // 一般使用同步提交，因为提交之后一般也没有什么逻辑代码了
                consumer.commitSync();

                // 手动异步提交offset，当前线程提交offset不会阻塞，可以继续处理后面的程序逻辑
                consumer.commitAsync(new OffsetCommitCallback() {
                    @Override
                    public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                        if (exception != null) {
                            System.err.println("Commit failed for " + offsets);
                            System.err.println("Commit failed exception: " + exception.getStackTrace());
                        }
                    }
                });

            }*/
        }
    }
}
