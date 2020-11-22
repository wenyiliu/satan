package com.satan.hadoop.cf.userCF;

import com.google.common.collect.Lists;
import com.satan.hadoop.config.HadoopConfiguration;
import com.satan.hadoop.constant.Constant;
import com.satan.hadoop.utils.CFUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author liuwenyi
 * @date 2020/11/19
 * com.satan.hadoop.cf.userCF.SimilarUserMapReduceJob
 */
public class SimilarUserMapReduceJob {

    private static int topN = 0;

    private static Map<String, String> userItemScoreMap = new HashMap<>(1000);

    public static class SimilarUserMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            String topNValue = configuration.get("topN");
            topN = StringUtils.isBlank(topNValue) ? Constant.ZERO : Integer.parseInt(topNValue);
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            Path path = inputSplit.getPath();
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] split = line.split(Constant.DEFAULT_SPLIT);
                userItemScoreMap.put(split[0], split[1]);
            }
            reader.close();
            if (userItemScoreMap == null || userItemScoreMap.isEmpty()) {
                throw new InterruptedException("simResult user item score is not empty");
            }
        }

        private static final Text VALUE_TEXT = new Text();
        private static final Text KEY_TEXT = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] userItemScoreMatrix = value.toString().split(Constant.DEFAULT_SPLIT);
            String user = userItemScoreMatrix[0];
            Map<String, String> similarUserItemScoreMap = CFUtil.initMap(userItemScoreMatrix[1]);
            String outputValue = userItemScoreMap.entrySet().stream().map(entry -> {
                if (entry.getKey().equals(user)) {
                    return null;
                }
                Map<String, String> entryMap = CFUtil.initMap(entry.getValue());
                double similar = CFUtil.similarUser(similarUserItemScoreMap, entryMap);
                if (Double.MAX_VALUE == similar) {
                    return null;
                }
                return entry.getKey() + "|" + similar;
            }).filter(Objects::nonNull).collect(Collectors.joining(Constant.COMMA));

            if (Constant.BLANK.equals(outputValue)) {
                return;
            }
            KEY_TEXT.set(user);
            VALUE_TEXT.set(outputValue);
            context.write(KEY_TEXT, VALUE_TEXT);
        }
    }

    public static class SimilarUserCombiner extends Reducer<Text, Text, Text, Text> {
        private static final List<List<String>> COMBINER_LIST = new ArrayList<>();
        private static final Text TEXT = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text combiner : values) {
                COMBINER_LIST.add(Lists.newArrayList(combiner.toString().split(Constant.COMMA)));
            }
            List<String> topList = CFUtil.getTopNList(topN, COMBINER_LIST);
            COMBINER_LIST.clear();
            String value = String.join(Constant.COMMA, topList);
            TEXT.set(value);
            context.write(key, TEXT);
        }
    }


    public static class SimilarUserReducer extends Reducer<Text, Text, Text, Text> {
        private static final List<List<String>> REDUCER_LIST = new ArrayList<>();
        private static final Text TEXT = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text t : values) {
                REDUCER_LIST.add(Lists.newArrayList(t.toString().split(Constant.COMMA)));
            }
            List<String> topList = CFUtil.getTopNList(topN, REDUCER_LIST);
            REDUCER_LIST.clear();
            String value = String.join(Constant.COMMA, topList);
            TEXT.set(value);
            context.write(key, TEXT);
        }
    }

    public static void runJob(String inputPath, String outputPath, int topN) throws Exception {
        Configuration configuration = HadoopConfiguration.getConfiguration();
        configuration.set("topN", String.valueOf(topN));
        Path inPath = new Path(inputPath);
        FileSystem fs = FileSystem.get(configuration);
        if (!fs.exists(inPath)) {
            throw new InterruptedException(inputPath + " is not exist");
        }
        Path path = new Path(outputPath);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        Job job = Job.getInstance(configuration);
        job.setJarByClass(SimilarUserMapReduceJob.class);
        job.setMapperClass(SimilarUserMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setCombinerClass(SimilarUserCombiner.class);
        job.setReducerClass(SimilarUserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, path);

        job.waitForCompletion(true);
    }

    public static void main(String[] args) throws Exception {
        String inputPath = "/user/root/cf/data/score_matrix/part-r-00000";
        String outputPath = "/user/root/cf/data/sim_result";
        runJob(inputPath, outputPath, 300);
    }
}
