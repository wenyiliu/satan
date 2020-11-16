package com.satan.hadoop.mr;

import com.satan.hadoop.config.HadoopConfiguration;
import com.satan.hadoop.utils.MapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author liuwenyi
 * @date 2020/11/09
 */
public class MinMaxMapReduceJob {

    public static class MinMaxMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String values = value.toString();
            String text = values.substring(8, 12);
            String outputValue = values.substring(values.length() - 4);
            context.write(new Text(text), new Text(outputValue));
        }
    }

    public static class MinMaxReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;
            for (Text value : values) {
                String[] texts = value.toString().split("\t");
                min = Math.min(min, Integer.parseInt(texts[0]));
                max = Math.max(max, Integer.parseInt(texts[1]));
            }
            if (max == 0 && min == 0) {
                return;
            }
            String minMaxValue = min + "\t" + max;
            context.write(key, new Text(minMaxValue));
        }
    }

    public static class MinMaxCombiner extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int max = Integer.MIN_VALUE;
            int min = Integer.MAX_VALUE;
            for (Text text : values) {
                int value = Integer.parseInt(text.toString());
                max = Math.max(max, value);
                min = Math.min(min, value);
            }
            if (max == 0 && min == 0) {
                return;
            }
            String minMaxValue = min + "\t" + max;
            context.write(key, new Text(minMaxValue));
        }
    }

    public static void runJob(String inputPath, String outputPath) throws Exception {
        MapReduceUtil.dealPath(inputPath, outputPath);
        org.apache.hadoop.conf.Configuration configuration = HadoopConfiguration.getConfiguration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(MinMaxMapReduceJob.class);
        job.setMapperClass(MinMaxMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(MinMaxCombiner.class);

        job.setReducerClass(MinMaxReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        MapReduceUtil.doRunJob(inputPath, outputPath, job);
    }

    public static void main(String[] args) throws Exception {
        String inputPath = "/user/root/mr_data/min_max.txt";
        String outputPath = "/user/root/mr_data/min_max_result.txt";
        if (args.length >= 2) {
            inputPath = args[0];
            outputPath = args[1];
        }
        runJob(inputPath, outputPath);
    }
}
