package com.satan.hadoop.mr;

import com.satan.hadoop.model.param.RunJobParam;
import com.satan.hadoop.model.result.Movie;
import com.satan.hadoop.utils.MapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author liuwenyi
 * @date 2020/11/16
 */
public class SortMapReduceJob {

    public static class SortMapper extends Mapper<LongWritable, Text, Movie, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(" ");
            if (values.length < 2) {
                return;
            }
            context.write(Movie.builder()
                    .name(values[0])
                    .score(Integer.parseInt(values[1]))
                    .build(), NullWritable.get());
        }
    }

    public static void runJob(String inputPath, String outputPath) throws Exception {
        MapReduceUtil.runJob(RunJobParam.builder()
                .inputPath(inputPath)
                .outputPath(outputPath)
                .jarClass(SortMapReduceJob.class)
                .mapperClass(SortMapper.class)
                .mapOutputKeyClass(Movie.class)
                .mapOutputValueClass(NullWritable.class)
                .build());
    }

    public static void main(String[] args) throws Exception {
        String inputPath = "/user/root/mr/data/sort_test/sort.txt";
        String outputPath = "/user/root/mr/data/sort_test/result";
        if (args.length >= 2) {
            inputPath = args[0];
            outputPath = args[1];
        }
        runJob(inputPath, outputPath);
    }
}
