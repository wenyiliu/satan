package com.satan.flink.job;

import com.satan.flink.utils.EnvUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liuwenyi
 * @date 2022/9/26
 **/
public class OrderJob {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = EnvUtils.getWebUIEnv();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop01:9092")
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setTopics("order")
                .setGroupId("test")
                .build();
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(),"order");
    }
}
