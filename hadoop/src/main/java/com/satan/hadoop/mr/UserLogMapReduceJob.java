package com.satan.hadoop.mr;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author liuwenyi
 * @date 2020/11/11
 */
public class UserLogMapReduceJob {


    public static class UserLogMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split(" ");
            String timestamp = StringUtils.isBlank(splits[2]) ? 0 + "" : splits[2];
            String deviceId = splits[3];
            String newsId = splits[4];
            String guid = splits[5];
            String user = guid + "|" + newsId + "|" + deviceId;
            String output = splits[1] + "|" + timestamp;
            context.write(new Text(user), new Text(output));
        }
    }

    public static class UserLogCombiner extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        }
    }


    public static void main(String[] args) {
    }
}
