package com.satan.hadoop.cf.userCF;

import com.satan.hadoop.config.HadoopConfiguration;
import com.satan.hadoop.constant.Constant;
import com.satan.hadoop.utils.HdfsUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author liuwenyi
 * @date 2020/11/19
 * 数据预处理，获取所有用户和所有商品
 */
public class PretreatmentMapReduceJob {

    private static int index = 0;

    public static class PretreatmentMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private final Text pretreatment = new Text();

        @Override
        protected void setup(Context context) {
            Configuration configuration = context.getConfiguration();
            String indexValue = configuration.get("index");
            index = StringUtils.isBlank(indexValue) ? index : Integer.parseInt(indexValue);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(Constant.DEFAULT_SPLIT);
            pretreatment.set(values[index]);
            context.write(pretreatment, NullWritable.get());
        }
    }

    public static class PretreatmentCombiner extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static class PretreatmentReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static void doRunJob(String inputPath, String outputPath, String index) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = HadoopConfiguration.getConfiguration();
        configuration.set("index", index);
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
        job.setJarByClass(PretreatmentMapReduceJob.class);

        job.setMapperClass(PretreatmentMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setCombinerClass(PretreatmentCombiner.class);

        job.setReducerClass(PretreatmentReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        job.waitForCompletion(true);
    }

    public static void runJob(String inputPath, String userOutputPath, String itemOutputPath) throws InterruptedException, IOException, ClassNotFoundException {
        String outputPath = "/tmp" + Integer.MAX_VALUE + "/output" + System.currentTimeMillis();
        doRunJob(inputPath, outputPath, "0");
        HdfsUtil.copy(outputPath, userOutputPath);
        doRunJob(inputPath, itemOutputPath, "1");
        HdfsUtil.copy(outputPath, itemOutputPath);
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        String inputPath = "/user/root/cf/data/ua.base";
        String userOutputPath = "/user/root/cf/data/result/user";
        String itemOutputPath = "/user/root/cf/data/result/item";
        runJob(inputPath, userOutputPath, itemOutputPath);
    }
}
