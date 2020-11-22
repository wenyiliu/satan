package com.satan.hadoop.cf.userCF;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.satan.hadoop.config.HadoopConfiguration;
import com.satan.hadoop.constant.Constant;
import com.satan.hadoop.utils.CFUtil;
import com.satan.hadoop.utils.HdfsUtil;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author liuwenyi
 * @date 2020/11/20
 * com.satan.hadoop.cf.userCF.UserCfMapReduceJob
 */
public class UserCfMapReduceJob {

    private static List<String> itemList = Lists.newArrayList();

    private static Map<String, String> userItemScoreMap = Maps.newHashMap();

    private static int topN = 0;

    public static class UserCfMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration configuration = context.getConfiguration();
            String topValue = configuration.get("topN");
            topN = StringUtils.isBlank(topValue) ? topN : Integer.parseInt(topValue);
            String itemPath = configuration.get("itemPath");
            if (StringUtils.isBlank(itemPath)) {
                throw new InterruptedException("itemPath is not exist");
            }
            FileSystem itemFs = FileSystem.get(configuration);
            itemList = HdfsUtil.getContextList(itemFs, itemPath);
            String userItemScorePath = configuration.get("userItemScorePath");
            if (StringUtils.isBlank(userItemScorePath)) {
                throw new InterruptedException("userItemScorePath is not exist");
            }
            FileSystem fs = FileSystem.get(configuration);
            userItemScoreMap = HdfsUtil.getContextList(fs, userItemScorePath).stream().collect(Collectors.toMap(
                    line -> line.split("\t")[0], line -> line.split("\t")[1]
            ));
        }

        private static final Text USER = new Text();
        private static final Text VALUE = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] values = value.toString().split(Constant.DEFAULT_SPLIT);
            USER.set(values[0]);
            String userItem = userItemScoreMap.get(values[0]);
            if (Objects.isNull(userItem)) {
                return;
            }
            // 用户已评分物品
            Map<String, String> userItemMap = CFUtil.initMap(userItem);
            // 该用户为评分物品
            List<String> userUnScoreItemList = itemList.stream()
                    .filter(s -> !userItemMap.containsKey(s))
                    .collect(Collectors.toList());
            if (userUnScoreItemList.isEmpty()) {
                return;
            }
            // 与该用户相似的用户以及相似度
            Map<String, String> similarUserMap = CFUtil.initMap(values[1]);
            List<String> recommendItemList = new ArrayList<>(itemList.size());
            for (String unScore : userUnScoreItemList) {
                BigDecimal score = BigDecimal.ZERO;
                BigDecimal similar = BigDecimal.ZERO;
                for (Map.Entry<String, String> similarUser : similarUserMap.entrySet()) {
                    String similarUserItem = userItemScoreMap.get(similarUser.getKey());
                    if (Objects.isNull(similarUserItem)) {
                        continue;
                    }
                    Map<String, String> similarUserItemScoreMap = CFUtil.initMap(similarUserItem);
                    String similarUserItemScore = similarUserItemScoreMap.get(unScore);
                    if (Objects.isNull(similarUserItemScore)) {
                        continue;
                    }
                    BigDecimal similarUserValue = BigDecimal.valueOf(Double.parseDouble(similarUser.getValue()));
                    score = score.add(BigDecimal.valueOf(Integer.parseInt(similarUserItemScore))
                            .multiply(similarUserValue));
                    similar = similar.add(similarUserValue);
                }
                if (score.compareTo(BigDecimal.ZERO) == 0 || similar.compareTo(BigDecimal.ZERO) == 0) {
                    recommendItemList.add(unScore + "|0.0");
                } else {
                    BigDecimal divide = score.divide(similar, 2);
                    recommendItemList.add(unScore + "|" + divide);
                }
            }
            if (recommendItemList.isEmpty()) {
                return;
            }
            List<String> topNList = CFUtil.getTopNList(topN, Collections.singletonList(recommendItemList));
            String recommendItemValue = String.join(Constant.COMMA, topNList);
            VALUE.set(recommendItemValue);
            context.write(USER, VALUE);
        }
    }

    public static class UserCfCombiner extends Reducer<Text, Text, Text, Text> {

        private static final Text VALUE = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String recommendItemValue = getRecommendItemValue(values);
            if (StringUtils.isBlank(recommendItemValue)) {
                return;
            }
            VALUE.set(recommendItemValue);
            context.write(key, VALUE);
        }
    }

    private static String getRecommendItemValue(Iterable<Text> values) {
        List<List<String>> recommendItemList = Lists.newArrayList();
        for (Text text : values) {
            recommendItemList.add(Lists.newArrayList(text.toString().split(Constant.COMMA)));
        }
        List<String> topList = CFUtil.getTopNList(topN, recommendItemList);
        return String.join(Constant.COMMA, topList);
    }

    public static class UserCfReducer extends Reducer<Text, Text, Text, Text> {

        private static final Text VALUE = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<List<String>> recommendItemList = Lists.newArrayList();
            for (Text text : values) {
                recommendItemList.add(Lists.newArrayList(text.toString().split(Constant.COMMA)));
            }
            String recommendItemValue = CFUtil.getTopNList(topN, recommendItemList)
                    .stream()
                    .map(item -> item.split("\\|")[0])
                    .distinct()
                    .collect(Collectors.joining(Constant.COMMA));
            VALUE.set(recommendItemValue);
            context.write(key, VALUE);
        }
    }

    public static boolean runJob(String inputPath, String outputPath, String itemPath, String userItemScorePath, int topN) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration configuration = HadoopConfiguration.getConfiguration();
        configuration.set("itemPath", itemPath);
        configuration.set("userItemScorePath", userItemScorePath);
        configuration.set("topN", String.valueOf(topN));
        FileSystem fileSystem = FileSystem.get(configuration);
        Path inPath = new Path(inputPath);
        if (!fileSystem.exists(inPath)) {
            throw new InterruptedException(inputPath + " is not exist");
        }
        Path outPath = new Path(outputPath);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
        }
        Job job = Job.getInstance(configuration);
        job.setJarByClass(UserCfMapReduceJob.class);
        job.setJobName("user-cf");

        job.setMapperClass(UserCfMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(UserCfCombiner.class);

        job.setReducerClass(UserCfReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        return job.waitForCompletion(true);
    }

    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        String inputPath = "/user/root/cf/data/sim_result/part-r-00000";
        String outputPath = "/user/root/cf/data/cf_result";
        String itemPath = "/user/root/cf/data/result/item";
        String userItemScorePath = "/user/root/cf/data/score_matrix/part-r-00000";
        int topN = 200;
        runJob(inputPath, outputPath, itemPath, userItemScorePath, topN);
    }
}
