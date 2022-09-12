package org.satan.flink11.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author liuwenyi
 * @date 2022/9/8
 **/
public class KafkaSource {


    public static FlinkKafkaConsumer<String> getConsumer(String topic) {
        Properties props = new Properties();
        // kafka broker地址
        props.put("bootstrap.servers", "hadoop01:9092,hadoop02:9092,hadoop03:9092");
        // 消费者组
        props.put("group.id", "flink_kafka_consumer");
        // 自动偏移量提交
        props.put("enable.auto.commit", true);
        // 偏移量提交的时间间隔，毫秒
        props.put("auto.commit.interval.ms", 5000);
        // kafka 消息的key序列化器
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // kafka 消息的value序列化器
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // 指定kafka的消费者从哪里开始消费数据
        // 共有三种方式，
        // #earliest
        // 当各分区下有已提交的offset时，从提交的offset开始消费；
        // 无提交的offset时，从头开始消费
        // #latest
        // 当各分区下有已提交的offset时，从提交的offset开始消费；
        // 无提交的offset时，消费新产生的该分区下的数据
        // #none
        // topic各分区都存在已提交的offset时，
        // 从offset后开始消费；
        // 只要有一个分区不存在已提交的offset，则抛出异常
        props.put("auto.offset.reset", "latest");
        return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), props);
    }


    public static FlinkKafkaConsumer<String> getConsumer(String topic, Properties properties) {
        return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
    }
}
