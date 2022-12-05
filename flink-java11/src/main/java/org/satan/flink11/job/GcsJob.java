package org.satan.flink11.job;

import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.satan.flink11.source.KafkaSource;

/**
 * @author liuwenyi
 * @date 2022/9/18
 **/
public class GcsJob {
    public static void main(String[] args) {
        final String topic = "order";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(50000L);
        env.setStateBackend((StateBackend) new FsStateBackend("file:////Users/liuwenyi/IdeaProjects/satan/data/checkpoint_result/flink11/order"));

        // 获取 kafka 中订单数据
        DataStream<String> dataStream4Kafka = env.addSource(KafkaSource.getConsumer(topic));

        dataStream4Kafka.writeAsText("gs://<bucket>/<endpoint>");
    }
}
