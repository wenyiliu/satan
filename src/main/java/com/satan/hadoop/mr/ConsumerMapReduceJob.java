package com.satan.hadoop.mr;

import com.satan.hadoop.config.HadoopConfiguration;
import com.satan.hadoop.model.result.ConsumerBO;
import com.satan.hadoop.utils.MapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Objects;

/**
 * @author liuwenyi
 * @date 2020/11/15
 */
public class ConsumerMapReduceJob {


    public static class ConsumerMapper extends Mapper<LongWritable, Text, Text, ConsumerBO> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(" ");
            if (values.length < 4) {
                return;
            }
            context.write(new Text(values[2]), ConsumerBO.builder()
                    .phone(values[0])
                    .address(values[1])
                    .name(values[2])
                    .consume(Long.parseLong(values[3]))
                    .build());
        }
    }

    public static class ConsumerCombiner extends Reducer<Text, ConsumerBO, Text, ConsumerBO> {
        @Override
        protected void reduce(Text key, Iterable<ConsumerBO> values, Context context) throws IOException, InterruptedException {
            Long consumeTotal = 0L;
            ConsumerBO consumer = null;
            for (ConsumerBO value : values) {
                consumeTotal += value.getConsume();
                if (Objects.isNull(consumer)) {
                    consumer = value;
                }
            }
            if (Objects.isNull(consumer)) {
                return;
            }
            consumer.setConsume(consumeTotal);
            context.write(key, consumer);
        }
    }

    public static class ConsumerReducer extends Reducer<Text, ConsumerBO, Text, ConsumerBO> {
        @Override
        protected void reduce(Text key, Iterable<ConsumerBO> values, Context context) throws IOException, InterruptedException {
            Long consumeTotal = 0L;
            ConsumerBO consumer = null;
            for (ConsumerBO value : values) {
                consumeTotal += value.getConsume();
                if (Objects.isNull(consumer)) {
                    consumer = value;
                }
            }
            if (Objects.isNull(consumer)) {
                return;
            }
            consumer.setConsume(consumeTotal);
            context.write(key, consumer);
        }
    }

    public static class ConsumerPartitioner extends Partitioner<Text, ConsumerBO> {

        @Override
        public int getPartition(Text text, ConsumerBO consumerBO, int numPartitions) {
            int partition;
            switch (consumerBO.getAddress()) {
                case "bj":
                    partition = 0;
                    break;
                case "sh":
                    partition = 1;
                    break;
                case "sz":
                    partition = 2;
                    break;
                default:
                    partition = 3;
            }
            return partition;
        }
    }

    public static void runJob(String inputPath, String outputPath) throws Exception {
        MapReduceUtil.dealPath(inputPath, outputPath);
        Job job = Job.getInstance(HadoopConfiguration.getConfiguration());
        job.setJobName("consumer");
        job.setJarByClass(ConsumerMapReduceJob.class);
        job.setMapperClass(ConsumerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(ConsumerBO.class);
        job.setPartitionerClass(ConsumerPartitioner.class);
        job.setCombinerClass(ConsumerCombiner.class);
        job.setReducerClass(ConsumerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(ConsumerBO.class);
        job.setNumReduceTasks(4);
        MapReduceUtil.doRunJob(inputPath, outputPath, job);
    }

    public static void main(String[] args) throws Exception {
        String inputPath = "/user/root/mr/data/partition/partition.txt";
        String outputPath = "/user/root/mr/data/partition/result";
        if (args.length >= 2) {
            inputPath = args[0];
            outputPath = args[1];
        }
        runJob(inputPath, outputPath);
    }
}
