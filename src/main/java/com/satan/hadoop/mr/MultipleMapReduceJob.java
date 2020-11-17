package com.satan.hadoop.mr;

import com.satan.hadoop.config.HadoopConfiguration;
import com.satan.hadoop.model.result.Company;
import com.satan.hadoop.utils.MapReduceUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Random;

/**
 * @author liuwenyi
 * @date 2020/11/16
 */
public class MultipleMapReduceJob {

    public static class MultipleMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(" ");
            if (values.length < 4) {
                return;
            }
            float profit = Float.parseFloat(values[2]) - Float.parseFloat(values[3]);
            context.write(new Text(values[0] + "|" + values[1]), new FloatWritable(profit));
        }
    }

    public static class MultipleReducer extends Reducer<Text, FloatWritable, NullWritable, Text> {

        @Override
        protected void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
            float total = 0f;
            for (FloatWritable c : values) {
                total += c.get();
            }
            context.write(NullWritable.get(), new Text(key + "|" + total));
        }
    }

    public static class MultipleMapper2 extends Mapper<LongWritable, Text, Company, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split("\\|");
            if (values.length < 3) {
                return;
            }
            Company company = new Company();
            company.setQuarterly(Integer.parseInt(values[0]));
            company.setName(values[1]);
            company.setProfit(Float.parseFloat(values[2]));
            context.write(company, NullWritable.get());
        }
    }

    public static void runJob(String inputPath, String outputPath) throws Exception {
        // 临时输出文件。确定该文件时，不要与现有目录有关，避免后续删除有用文件
        String tmpOutputPath = "/tmp" + new Random().nextInt(Integer.MAX_VALUE) + "/output" + System.currentTimeMillis();
        MapReduceUtil.dealPath(inputPath, tmpOutputPath);

        Job job1 = Job.getInstance(HadoopConfiguration.getConfiguration());

        job1.setJarByClass(MultipleMapReduceJob.class);

        job1.setMapperClass(MultipleMapper.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(FloatWritable.class);

        job1.setReducerClass(MultipleReducer.class);
        job1.setOutputKeyClass(NullWritable.class);
        job1.setOutputValueClass(Text.class);

        // 设置作业1输入文件和输出文件的路径
        FileInputFormat.setInputPaths(job1, new Path(inputPath));
        FileOutputFormat.setOutputPath(job1, new Path(tmpOutputPath));
        boolean result = job1.waitForCompletion(true);
        // 启动job2
        if (result) {
            Job job2 = Job.getInstance(HadoopConfiguration.getConfiguration());

            job2.setJarByClass(MultipleMapReduceJob.class);

            job2.setMapperClass(MultipleMapper2.class);
            job2.setMapOutputKeyClass(Company.class);
            job2.setMapOutputValueClass(NullWritable.class);

            FileSystem fileSystem = FileSystem.get(HadoopConfiguration.getConfiguration());

            if (fileSystem.exists(new Path(outputPath))) {
                fileSystem.delete(new Path(outputPath), true);
            }
            FileInputFormat.setInputPaths(job2, new Path(tmpOutputPath));
            FileOutputFormat.setOutputPath(job2, new Path(outputPath));
            boolean result1 = job2.waitForCompletion(true);
            // 删除临时文件
            FileSystem.get(HadoopConfiguration.getConfiguration()).deleteOnExit(new Path(tmpOutputPath));
            System.exit(result1 ? 0 : -1);
        }
    }

    public static void main(String[] args) throws Exception {
        runJob("/user/root/mr/data/muti_mr/mutil_mr.txt",
                "/user/root/mr/data/muti_mr/result");
    }
}
