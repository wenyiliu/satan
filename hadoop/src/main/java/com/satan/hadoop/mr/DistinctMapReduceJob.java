package com.satan.hadoop.mr;

import com.satan.hadoop.config.HadoopConfiguration;
import com.satan.hadoop.utils.MapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author liuwenyi
 * @date 2020/11/10
 */
public class DistinctMapReduceJob {

    public static class DistinctMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(value, NullWritable.get());
        }
    }

    public static class DistinctCombiner extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static class DistinctReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void runJob(String inputPath, String outputPath) throws Exception {
        MapReduceUtil.dealPath(inputPath, outputPath);
        Job job = Job.getInstance(HadoopConfiguration.getConfiguration());
        job.setJarByClass(DistinctMapReduceJob.class);

        job.setMapperClass(DistinctMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(DistinctCombiner.class);

        job.setReducerClass(DistinctReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MapReduceUtil.doRunJob(inputPath, outputPath, job);
    }

    public static void main(String[] args) throws Exception {
        String inputPath = "/user/root/mr_data/dup.txt";
        String outputPath = "/user/root/mr_data/dup_result.txt";
        if (args.length >= 2) {
            inputPath = args[0];
            outputPath = args[1];
        }
        runJob(inputPath, outputPath);
    }
}
