package com.satan.hadoop.mr;

import com.satan.hadoop.model.param.RunJobParam;
import com.satan.hadoop.model.result.Student;
import com.satan.hadoop.utils.MapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author liuwenyi
 * @date 2020/11/16
 */
public class MultipleFilesCombineMapReduceJob {

    public static class MultipleFilesCombineMapper extends Mapper<LongWritable, Text, Text, Student> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] values = value.toString().split(" ");
            if (values.length < 3) {
                return;
            }
            Student student = new Student();
            student.setName(values[1]);
            int score = Integer.parseInt(values[2]);
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            if ("chinese.txt".equals(fileName)) {
                student.setChScore(score);
            } else if ("english.txt".equals(fileName)) {
                student.setEnScore(score);
            } else if ("math.txt".equals(fileName)) {
                student.setMathScore(score);
            } else {
                return;
            }
            context.write(new Text(values[1]), student);
        }
    }

    public static class MultipleFilesCombiner extends Reducer<Text, Student, Text, Student> {
        @Override
        protected void reduce(Text key, Iterable<Student> values, Context context) throws IOException, InterruptedException {
            int chScore = 0;
            int enScore = 0;
            int mathScore = 0;
            for (Student s : values) {
                chScore += s.getChScore();
                enScore += s.getEnScore();
                mathScore += s.getMathScore();
            }
            context.write(key, new Student(key.toString(), chScore, enScore, mathScore));
        }
    }


    public static class MultipleFilesCombineReducer extends Reducer<Text, Student, Text, Student> {
        @Override
        protected void reduce(Text key, Iterable<Student> values, Context context) throws IOException, InterruptedException {
            int chScore = 0;
            int enScore = 0;
            int mathScore = 0;
            for (Student s : values) {
                chScore += s.getChScore();
                enScore += s.getEnScore();
                mathScore += s.getMathScore();
            }
            context.write(key, new Student(key.toString(), chScore, enScore, mathScore));
        }
    }

    public static void runJob(String inputJob, String outputPath) throws Exception {
        MapReduceUtil.runJob(RunJobParam.builder()
                .inputPath(inputJob)
                .outputPath(outputPath)
                .jarClass(MultipleFilesCombineMapReduceJob.class)
                .mapperClass(MultipleFilesCombineMapper.class)
                .mapOutputKeyClass(Text.class)
                .mapOutputValueClass(Student.class)
                .combinerClass(MultipleFilesCombiner.class)
                .reducerClass(MultipleFilesCombineReducer.class)
                .outputKeyClass(Text.class)
                .outputValueClass(Student.class)
                .build());
    }

    public static void main(String[] args) throws Exception {
        String inputPath = "/user/root/mr/data/m_file_test";
        String outputPath = "/user/root/mr/data/m_file_test/result";
        if (args.length >= 2) {
            inputPath = args[0];
            outputPath = args[1];
        }
        runJob(inputPath, outputPath);
    }
}
