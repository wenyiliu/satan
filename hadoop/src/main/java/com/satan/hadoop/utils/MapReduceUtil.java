package com.satan.hadoop.utils;

import com.satan.hadoop.config.HadoopConfiguration;
import com.satan.hadoop.model.param.RunJobParam;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.Objects;

/**
 * @author liuwenyi
 * @date 2020/11/10
 */
@SuppressWarnings("rawtypes")
public class MapReduceUtil {

    public static void dealPath(String inputPath, String outputPath) throws Exception {
        if (StringUtils.isBlank(inputPath) || StringUtils.isBlank(outputPath)) {
            String errorInfo = "the job param is interrupted,param1:" + inputPath + ",param2:" + outputPath;
            throw new InterruptedException(errorInfo);
        }
        FileSystem fileSystem = FileSystem.get(HadoopConfiguration.getConfiguration());
        if (!fileSystem.exists(new Path(inputPath))) {
            throw new InterruptedException("input path is not exists");
        }
        Path path = new Path(outputPath);
        if (fileSystem.exists(path)) {
            fileSystem.delete(path, true);
        }
    }

    public static void doRunJob(String inputPath, String outputPath, Job job) throws Exception {
        // 设置作业输入文件和输出文件的路径
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));
        // 将作业提交到群集并等待它完成，参数设置为 true 代表打印显示对应的进度
        boolean result = job.waitForCompletion(true);
        // 根据作业结果,终止当前运行的 Java 虚拟机,退出程序
//        System.exit(result ? 0 : -1);
    }

    private static Job job(Class<?> jarClass,
                           Class<? extends Mapper> mapperClass,
                           Class<? extends Reducer> reducerClass,
                           Class<? extends Reducer> combiner,
                           Class<? extends Writable> mapOutputKeyClass,
                           Class<? extends Writable> mapOutputValueClass,
                           Class<? extends Writable> outputKeyClass,
                           Class<? extends Writable> outputValueClass,
                           Class<? extends Partitioner> partitionClass) throws Exception {
        Job job = Job.getInstance(HadoopConfiguration.getConfiguration());
        job.setJarByClass(jarClass);
        job.setMapperClass(mapperClass);
        if (Objects.nonNull(reducerClass)) {
            job.setReducerClass(reducerClass);
        }
        if (Objects.nonNull(combiner)) {
            job.setCombinerClass(combiner);
        }
        if (Objects.nonNull(partitionClass)) {
            job.setPartitionerClass(partitionClass);
        }
        job.setMapOutputKeyClass(mapOutputKeyClass);
        job.setMapOutputValueClass(mapOutputValueClass);
        if (Objects.nonNull(outputKeyClass)) {
            job.setOutputKeyClass(outputKeyClass);
        }
        if (Objects.nonNull(outputValueClass)) {
            job.setOutputValueClass(outputValueClass);
        }
        return job;
    }

    public static Job job(Class<?> jarClass,
                          Class<? extends Mapper> mapperClass,
                          Class<? extends Reducer> reducerClass,
                          Class<? extends Writable> mapOutputKeyClass,
                          Class<? extends Writable> mapOutputValueClass,
                          Class<? extends Writable> outputKeyClass,
                          Class<? extends Writable> outputValueClass) throws Exception {
        return job(jarClass, mapperClass, reducerClass, null, mapOutputKeyClass, mapOutputValueClass,
                outputKeyClass, outputValueClass, null);
    }


    public static void runJob(RunJobParam param) throws Exception {
        if (Objects.isNull(param)) {
            throw new InterruptedException("the param is not null");
        }
        dealPath(param.getInputPath(), param.getOutputPath());
        Job job = job(param.getJarClass(), param.getMapperClass(), param.getReducerClass(), param.getCombinerClass(),
                param.getMapOutputKeyClass(), param.getMapOutputValueClass(), param.getOutputKeyClass(),
                param.getOutputValueClass(), param.getPartitionClass());
        if (Objects.nonNull(param.getJobName())) {
            job.setJobName(param.getJobName());
        }
        doRunJob(param.getInputPath(), param.getOutputPath(), job);
    }

}
