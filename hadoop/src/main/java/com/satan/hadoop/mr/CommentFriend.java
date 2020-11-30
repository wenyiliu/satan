package com.satan.hadoop.mr;

import com.satan.hadoop.config.HadoopConfiguration;
import com.satan.hadoop.constant.Constant;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

/**
 * @author liuwenyi
 * @date 2020/11/16
 */
public class CommentFriend {

    public static class FriendMapper1 extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(":");
            if (values.length < Constant.TWO) {
                return;
            }
            String user = values[0];
            String[] friends = values[1].split(",");
            outValue.set(user);
            for (String friend : friends) {
                outKey.set(friend);
                context.write(outKey, outValue);
            }
        }
    }

    public static class FriendReducer1 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String> list = new ArrayList<>();
            for (Text text : values) {
                list.add(text.toString());
            }
            String friends = String.join(",", list);
            context.write(key, new Text(friends));
        }
    }

    public static class FriendMapper extends Mapper<LongWritable, Text, Text, Text> {
        private final Text outKey = new Text();
        private final Text outValue = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\t");
            if (values.length < Constant.TWO) {
                return;
            }
            String user = values[0];
            String[] friends = values[1].split(",");
            int length = friends.length;
            outValue.set(user);
            Arrays.sort(friends);
            for (int i = 0; i < length - 2; i++) {
                String friendI = friends[i];
                for (int j = i + 1; j < length - 1; j++) {
                    outKey.set(friendI + "-" + friends[j]);
                    context.write(outKey, outValue);
                }
            }
        }
    }

    public static class FriendReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> set = new HashSet<>();
            for (Text text : values) {
                set.add(text.toString());
            }
            String friends = String.join(",", set);
            context.write(key, new Text(friends));
        }
    }

    public static void runJob(String inputPath, String outputPath) throws Exception {
        // 创建中间结果文件，创建时要尽量保证目录是新的，不会与已有目录冲突
        String tmp = "/tmp" + new Random().nextInt(Integer.MAX_VALUE) + "/friend" + System.currentTimeMillis();

        // 获取 Hadoop 文件对象
        FileSystem fs = FileSystem.get(HadoopConfiguration.getConfiguration());
        Path inPath = new Path(inputPath);
        Path tmpPath = new Path(tmp);
        Path outPath = new Path(outputPath);

        // 校验是否存在输入文件
        if (!fs.exists(inPath)) {
            throw new Exception("the " + inputPath + " is not exist");
        }
        // 如果临时文件已存在，则删除
        if (fs.exists(tmpPath)) {
            fs.delete(tmpPath, true);
        }

        // 如果输出文件已存在，删除
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }
        // 配置第一个 job
        Job job1 = Job.getInstance(HadoopConfiguration.getConfiguration());

        // jar
        job1.setJarByClass(CommentFriend.class);

        // mapper
        job1.setMapperClass(FriendMapper1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);

        // reducer
        job1.setReducerClass(FriendReducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        // 输入和输出地址
        FileInputFormat.setInputPaths(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, tmpPath);

        // 判断 job1 是否正常结束，如果是 进行第二个job
        if (job1.waitForCompletion(true)) {
            // 配置第二个 job
            Job job2 = Job.getInstance(HadoopConfiguration.getConfiguration());

            // jar
            job2.setJarByClass(CommentFriend.class);

            // mapper
            job2.setMapperClass(FriendMapper.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);

            // reducer
            job2.setReducerClass(FriendReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            // 输入和输出地址，job1 输出的目录作为 job2 的输入目录
            FileInputFormat.setInputPaths(job2, tmpPath);
            FileOutputFormat.setOutputPath(job2, outPath);

            if (job2.waitForCompletion(true)) {
                // 删除临时目录
                fs.delete(tmpPath, true);
                // 关闭文件系统
                fs.close();
                // 正常退出 JVM
                System.exit(0);
            } else {
                // job2 非正常结束，退出 JVM，exit code 为 255
                System.exit(255);
            }
        } else {
            // job1 非正常结束，退出 JVM，exit code 为 255
            System.exit(255);
        }
    }

    public static void main(String[] args) throws Exception {
        String inputPath = "/user/root/mr/data/findFriend/friedn_test.txt";
        String outputPath = "/user/root/mr/data/findFriend/result2";
        runJob(inputPath, outputPath);
    }
}
