package org.satan.flink11;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

/**
 * @author liuwenyi
 * @date 2022/9/9
 **/
public class FlinkRoundRobinPartitioner<T> extends FlinkKafkaPartitioner<T> {

    @Override
    public int partition(T t, byte[] bytes, byte[] bytes1, String s, int[] ints) {
        return 0;
    }
}
