package com.satan.hadoop.mr;

import com.google.common.collect.Lists;
import com.satan.hadoop.model.param.RunJobParam;
import com.satan.hadoop.utils.CommonUtil;
import com.satan.hadoop.utils.MapReduceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

    private static final IntWritable ONE = new IntWritable(1);

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, final Context context) {
            String textValue = value.toString();
            if (StringUtils.isBlank(textValue)) {
                return;
            }
            List<String> valueList = Lists.newArrayList(CommonUtil.replacePunctuation(textValue).split(" "));
            valueList.forEach(s -> {
                try {
                    context.write(new Text(s), ONE);
                } catch (Exception e) {
                    LOG.error("map 读取数据失败 ", e);
                }
            });
        }
    }

    public static class WordCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count += value.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0L;
            for (IntWritable value : values) {
                count += value.get();
            }
            System.out.println(key.toString() + "--" + count);
            context.write(key, new LongWritable(count));
        }
    }

    public static void runJob(String inputPath, String outputPath) throws Exception {
        RunJobParam build = RunJobParam.builder()
                .inputPath(inputPath)
                .outputPath(outputPath)
                .mapperClass(WordCountMapper.class)
                .mapOutputKeyClass(Text.class)
                .mapOutputValueClass(IntWritable.class)
                .combinerClass(WordCountCombiner.class)
                .reducerClass(WordCountReducer.class)
                .outputKeyClass(Text.class)
                .outputValueClass(LongWritable.class)
                .build();
        MapReduceUtil.runJob(build);
    }

    public static void main(String[] args) throws Exception {
        String inputPath = "/user/root/order.txt";
        String outputPath = "/user/root/order_result";
        if (args.length >= 2) {
            inputPath = args[0];
            outputPath = args[1];
        }
        runJob(inputPath, outputPath);
    }
}
