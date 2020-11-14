package com.satan.hadoop.mr;

import com.google.common.collect.Lists;
import com.satan.hadoop.config.HadoopConfiguration;
import com.satan.hadoop.utils.CommonUtil;
import com.satan.hadoop.utils.MapReduceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * @author liuwenyi
 * @date 2020/11/09
 */
public class WordCountMapReduceJob {

    private static final Logger LOG = LoggerFactory.getLogger(WordCountMapReduceJob.class);

    public static class WordMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, final Context context) {
            String textValue = value.toString();
            if (StringUtils.isBlank(textValue)) {
                return;
            }
            List<String> valueList = Lists.newArrayList(CommonUtil.replacePunctuation(textValue).split(" "));
            valueList.forEach(s -> {
                try {
                    context.write(new Text(s), new IntWritable(1));
                } catch (Exception e) {
                    LOG.error("map 读取数据失败 ", e);
                }
            });
        }
    }

    public static class WordReduce extends Reducer<Text, IntWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0L;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(key, new LongWritable(count));
        }
    }

    public static void runJob(String inputPath, String outputPath) throws Exception {
        MapReduceUtil.dealPath(inputPath, outputPath);
        Job job = Job.getInstance(HadoopConfiguration.getConfiguration());
        job.setJarByClass(WordCountMapReduceJob.class);
        job.setMapperClass(WordMap.class);
        job.setReducerClass(WordReduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        MapReduceUtil.doRunJob(inputPath, outputPath, job);
    }

    public static void main(String[] args) throws Exception {
        String inputPath = "/user/root/mr/word_count/input.txt";
        String outputPath = "/user/root/mr/word_count/output.txt";
        if (args.length >= 2) {
            inputPath = args[0];
            outputPath = args[1];
        }
        runJob(inputPath, outputPath);
    }
}
