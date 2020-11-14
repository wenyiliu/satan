package com.satan.hadoop.mr;

import com.satan.hadoop.config.HadoopConfiguration;
import com.satan.hadoop.utils.MapReduceUtil;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;

/**
 * @author liuwenyi
 * @date 2020/11/09
 */
public class AvgScoreMapReduceJob {

    public static class AvgScoreMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String textValue = value.toString();
            String[] values = textValue.split(" ");
            if (values.length < 2) {
                return;
            }
            context.write(new Text(values[0]), new Text(values[1]));
        }
    }

    public static class AvgScoreReduce extends Reducer<Text, Text, Text, FloatWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long total = 0;
            long nums = 0;
            for (Text text : values) {
                String[] textValues = text.toString().split("\t");
                total += Long.parseLong(textValues[0]);
                nums += Long.parseLong(textValues[1]);
            }
            float avg = BigDecimal.valueOf(total).divide(BigDecimal.valueOf(nums),3, BigDecimal.ROUND_HALF_UP)
                    .setScale(3, BigDecimal.ROUND_HALF_UP).floatValue();
            context.write(key, new FloatWritable(avg));
        }
    }

    public static class AvgScoreCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long scoreTotal = 0L;
            long countNums = 0L;
            for (Text writable : values) {
                scoreTotal += Integer.parseInt(writable.toString());
                countNums++;
            }
            String outputValue = scoreTotal + "\t" + countNums;
            context.write(key, new Text(outputValue));
        }
    }

    public static void runJob(String inputPath, String outputPath) throws Exception {
        MapReduceUtil.dealPath(inputPath, outputPath);
        Job job = Job.getInstance(HadoopConfiguration.getConfiguration());
        job.setJarByClass(AvgScoreMapReduceJob.class);
        job.setMapperClass(AvgScoreMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(AvgScoreCombiner.class);

        job.setReducerClass(AvgScoreReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        MapReduceUtil.doRunJob(inputPath, outputPath, job);
    }

    public static void main(String[] args) throws Exception {
        String inputPath = "/user/root/mr_data/avg.txt";
        String outputPath = "/user/root/mr_data/avg_result.txt";
        if (args.length >= 2) {
            inputPath = args[0];
            outputPath = args[1];
        }
        runJob(inputPath, outputPath);
    }
}
