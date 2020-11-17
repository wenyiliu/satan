package com.satan.hadoop.mr;

import com.satan.hadoop.config.HadoopConfiguration;
import com.satan.hadoop.model.param.RunJobParam;
import com.satan.hadoop.model.result.ConsumerBO;
import com.satan.hadoop.utils.MapReduceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author liuwenyi
 * @date 2020/11/16
 */
public class SortTotalMapReduceJob {

    public static class SortTotalMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            Text text = new Text();
            String[] values = value.toString().split(" ");
            for (String s : values) {
                if (StringUtils.isBlank(s)) {
                    continue;
                }
                text.set(s.trim());
                context.write(text, new IntWritable(1));
            }
        }
    }

    public static class SortTotalPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
            int length = text.toString().length();
            if (length <= 2) {
                return 0;
            } else if (length <= 3) {
                return 1;
            } else {
                return 2;
            }
        }
    }

    public static class SortTotalReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(key, new LongWritable(count));
        }
    }

    public static void runJob(String inputPath, String outputPath) throws Exception {
        MapReduceUtil.dealPath(inputPath, outputPath);
        Job job = Job.getInstance(HadoopConfiguration.getConfiguration());
        job.setJobName("sort total");
        job.setJarByClass(SortTotalMapReduceJob.class);
        job.setMapperClass(SortTotalMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(SortTotalReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        job.setPartitionerClass(SortTotalPartitioner.class);
        job.setNumReduceTasks(3);
        MapReduceUtil.doRunJob(inputPath, outputPath, job);
    }

    public static void main(String[] args) throws Exception {
        String inputPath = "/user/root/mr/data/sort_test/sort2.txt";
        String outputPath = "/user/root/mr/data/sort_test/result2";
        if (args.length >= 2) {
            inputPath = args[0];
            outputPath = args[1];
        }
        runJob(inputPath, outputPath);
    }
}
