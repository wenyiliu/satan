package com.satan.hadoop.partition;

import com.satan.hadoop.Test;
import com.satan.hadoop.model.result.ConsumerBO;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author liuwenyi
 * @date 2020/11/15
 */
public class ConsumerPartitioner extends Partitioner<Text, ConsumerBO> {

    @Override
    public int getPartition(Text text, ConsumerBO consumerBO, int numPartitions) {
        return 0;
    }
}
