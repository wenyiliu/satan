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
        return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), new Properties());
    }


    public static FlinkKafkaConsumer<String> getConsumer(String topic, Properties properties) {
        return new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
    }
}
