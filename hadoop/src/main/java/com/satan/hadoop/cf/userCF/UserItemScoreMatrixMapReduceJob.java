package com.satan.hadoop.cf.userCF;

import com.google.common.collect.Lists;
import com.satan.hadoop.config.HadoopConfiguration;
import com.satan.hadoop.constant.Constant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author liuwenyi
 * @date 2020/11/19
 */
public class UserItemScoreMatrixMapReduceJob {

    public static class UserItemScoreMatrixMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Text user = new Text();
        private final Text itemScore = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(Constant.DEFAULT_SPLIT);
            user.set(values[0]);
            String itemScoreValue = values[1] + "|" + values[2];
            itemScore.set(itemScoreValue);
            context.write(user, itemScore);
        }
    }

    public static class UserItemScoreMatrixCombiner extends Reducer<Text, Text, Text, Text> {
        List<String> valueList = Lists.newArrayList();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text t : values) {
                valueList.add(t.toString());
            }
            String value = valueList.stream()
                    .distinct()
                    .collect(Collectors.joining(Constant.COMMA));
            context.write(key, new Text(value));
        }
    }

    public static class UserItemScoreMatrixReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> list = new ArrayList<>();
            for (Text t : values) {
                list.addAll(Lists.newArrayList(t.toString().split(Constant.COMMA)));
            }
            String collect = list.stream()
                    .distinct()
                    .collect(Collectors.joining(Constant.COMMA));
            context.write(key, new Text(collect));
        }
    }

    public static void runJob(String inputPath, String outputPath) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = HadoopConfiguration.getConfiguration();
        FileSystem fs = FileSystem.get(configuration);
        Path inPath = new Path(inputPath);
        if (!fs.exists(inPath)) {
            throw new IOException(inputPath + " is not exist");
        }
        Path outPath = new Path(outputPath);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        Job job = Job.getInstance(configuration);
        job.setJobName("user-item-score-matrix");
        job.setJarByClass(UserItemScoreMatrixMapReduceJob.class);

        job.setMapperClass(UserItemScoreMatrixMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(UserItemScoreMatrixCombiner.class);

        job.setReducerClass(UserItemScoreMatrixReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        String inputPath = "/user/root/cf/data/ua.base";
        String outputPath = "/user/root/cf/data/score_matrix";
        runJob(inputPath, outputPath);
    }
}
