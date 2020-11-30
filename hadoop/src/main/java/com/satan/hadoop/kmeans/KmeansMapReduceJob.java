package com.satan.hadoop.kmeans;

import com.google.common.collect.Lists;
import com.satan.hadoop.config.HadoopConfiguration;
import com.satan.hadoop.constant.Constant;
import com.satan.hadoop.model.param.RunJobParam;
import com.satan.hadoop.utils.DistanceUtil;
import com.satan.hadoop.utils.HdfsUtil;
import com.satan.hadoop.utils.KmeansUtil;
import com.satan.hadoop.utils.MapReduceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * @author liuwenyi
 * @date 2020/11/17
 */
public class KmeansMapReduceJob {

    private static final Logger log = LoggerFactory.getLogger(KmeansMapReduceJob.class);

    private static int clusterNums = 3;

    private static Map<String, String[]> clusterCenterMap;

    public static class InitClusterMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void setup(Context context) {
            Configuration configuration = context.getConfiguration();
            clusterNums = StringUtils.isBlank(configuration.get(Constant.DEFAULT_CLUSTER_NUMS))
                    ? clusterNums
                    : Integer.parseInt(configuration.get(Constant.DEFAULT_CLUSTER_NUMS));
            clusterCenterMap = new HashMap<>(clusterNums << 1);
        }

        private final Random random = new Random();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            int center = (int) (key.get() + random.nextLong()) % clusterNums + 1;
            if (!clusterCenterMap.containsKey(String.valueOf(center)) && center > 0) {
                String v = value.toString().substring(value.toString().indexOf(Constant.COMMA) + 1);
                context.write(new Text(String.valueOf(center)), new Text(v));
            }
        }
    }

    public static class InitClusterReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Text> textList = getTextList(values);
            int len = textList.size();
            Text text = textList.get(0);
            int length = text.toString().split(Constant.COMMA).length;
            double[] total = new double[length];
            for (Text value : textList) {
                String[] items = value.toString().split(Constant.COMMA);
                int min = Math.min(length, items.length);
                for (int i = 0; i < min; i++) {
                    total[i] += NumberUtils.toDouble(items[i]) / len;
                }
            }
            String value = Arrays.stream(total)
                    .mapToObj(String::valueOf)
                    .collect(Collectors.joining(Constant.COMMA));
            text.set(value);
            context.write(key, text);
        }
    }

    public static class KmeansMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException {
            Configuration configuration = context.getConfiguration();
            FileSystem fileSystem = FileSystem.get(configuration);
            String path = configuration.get(Constant.CLUSTER_CENTER_PATH);
            clusterNums = StringUtils.isBlank(configuration.get(Constant.DEFAULT_CLUSTER_NUMS))
                    ? clusterNums
                    : Integer.parseInt(configuration.get(Constant.DEFAULT_CLUSTER_NUMS));
            if (StringUtils.isNotBlank(path)) {
                clusterCenterMap = KmeansUtil.getClusterCenterMap(fileSystem, path);
            } else {
                clusterCenterMap = new HashMap<>(clusterNums);
            }

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String subValue = value.toString().substring(value.toString().indexOf(Constant.COMMA) + 1);
            String[] values = subValue.split(Constant.COMMA);
            double distinct = Double.MAX_VALUE;
            String cluster = Constant.BLANK;
            for (Map.Entry<String, String[]> entry : clusterCenterMap.entrySet()) {
                double v = DistanceUtil.euclideanDistance(values, entry.getValue());
                if (v < distinct) {
                    distinct = v;
                    cluster = entry.getKey();
                }
            }
            if (Constant.BLANK.equals(cluster)) {
                return;
            }
            context.write(new Text(cluster), new Text(subValue));
        }
    }

    public static class KmeansCombiner extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<Text> textList = getTextList(values);
            int nums = textList.size();
            Text text = textList.get(0);
            if (Objects.isNull(text)) {
                return;
            }
            int length = text.toString().split(Constant.COMMA).length;
            double[] total = new double[length];
            for (Text value : textList) {
                String[] items = value.toString().split(Constant.COMMA);
                int min = Math.min(length, items.length);
                for (int i = 0; i < min; i++) {
                    total[i] += NumberUtils.toDouble(items[i]);
                }
            }
            String value = Arrays.stream(total)
                    .mapToObj(String::valueOf)
                    .collect(Collectors.joining(Constant.COMMA));
            text.set(value + Constant.DEFAULT_SPLIT + nums);
            context.write(key, text);
        }
    }

    private static List<Text> getTextList(Iterable<Text> values) {
        List<Text> textList = Lists.newArrayList();
        for (Text t : values) {
            textList.add(t);
        }
        return textList;
    }

    public static class KmeansReducer extends Reducer<Text, Text, Text, Text> {

        private final Text text = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int nums = 0;
            List<Text> textList = getTextList(values);
            int length = textList.get(0).toString().split(Constant.COMMA).length;
            double[] center = new double[length];
            for (Text t : textList) {
                String[] tValue = t.toString().split(Constant.DEFAULT_SPLIT);
                String[] textValue = tValue[0].split(Constant.COMMA);
                int min = Math.min(textValue.length, length);
                for (int i = 0; i < min; i++) {
                    center[i] += NumberUtils.toDouble(textValue[i]);
                }
                nums += Integer.parseInt(tValue[1]);
            }
            int finalNums = nums;
            String value = Arrays.stream(center)
                    .map(d -> d / finalNums)
                    .mapToObj(String::valueOf)
                    .collect(Collectors.joining(Constant.COMMA));
            text.set(value);
            context.write(key, text);
        }
    }

    public static void runJob(String inputPath, String outputPath, int begin, String clusterPath) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = HadoopConfiguration.getConfiguration();
        configuration.set(Constant.CLUSTER_CENTER_PATH, clusterPath);
        configuration.set(Constant.DEFAULT_CLUSTER_NUMS, "3");
        FileSystem fileSystem = FileSystem.get(configuration);
        Path inPath = new Path(inputPath);
        if (!fileSystem.exists(inPath)) {
            throw new IOException(inputPath + " is not exist");
        }
        Path path = new Path(outputPath);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
        Job job = Job.getInstance(configuration);
        job.setJobName("train " + begin);
        log.info("第 " + begin + " 次训练");
        job.setJarByClass(KmeansMapReduceJob.class);

        job.setMapperClass(KmeansMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(KmeansCombiner.class);

        job.setReducerClass(KmeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, path);
        job.waitForCompletion(true);
    }

    private static void train(String inputPath, String outputPath, String tmpPath, int maxIter, double rate) throws Exception {
        MapReduceUtil.runJob(RunJobParam.builder()
                .inputPath(inputPath)
                .outputPath(outputPath)
                .jarClass(KmeansMapReduceJob.class)
                .mapperClass(InitClusterMapper.class)
                .mapOutputKeyClass(Text.class)
                .mapOutputValueClass(Text.class)
                .reducerClass(InitClusterReduce.class)
                .outputKeyClass(Text.class)
                .outputValueClass(Text.class)
                .build());
        HdfsUtil.copy(outputPath, tmpPath);
        int begin = 0;
        while (begin < maxIter) {
            runJob(inputPath, outputPath, begin, tmpPath);
            boolean finished = KmeansUtil.isFinished(tmpPath, outputPath, rate);
            if (finished) {
                break;
            }
            begin++;
            if (begin != maxIter) {
                HdfsUtil.copy(outputPath, tmpPath);
            } else {
                runJob(tmpPath, outputPath, begin, tmpPath);
                HdfsUtil.deletePath(tmpPath);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        String inputPath = "/user/root/mr/data/kmeans/user_info.txt";
        String outputPath = "/user/root/mr/data/kmeans/result";
        String tmpPath = "/user/root/mr/data/kmeans/tmp/center.txt";
        train(inputPath, outputPath, tmpPath, 100, 0.01);
    }
}
