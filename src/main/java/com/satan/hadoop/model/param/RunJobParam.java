package com.satan.hadoop.model.param;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author liuwenyi
 * @date 2020/11/14
 */
@SuppressWarnings("rawtypes")
public class RunJobParam {

    private final String inputPath;

    private final String outputPath;

    private final String jobName;

    private final Class<?> jarClass;

    private final Class<? extends Mapper> mapperClass;

    private final Class<? extends Reducer> reducerClass;

    private final Class<? extends Reducer> combinerClass;

    private final Class<? extends Writable> mapOutputKeyClass;

    private final Class<? extends Writable> mapOutputValueClass;

    private final Class<? extends Writable> outputKeyClass;

    private final Class<? extends Writable> outputValueClass;

    private final Class<? extends Partitioner> partitionClass;

    public Class<? extends Partitioner> getPartitionClass() {
        return partitionClass;
    }

    public String getInputPath() {
        return inputPath;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public String getJobName() {
        return jobName;
    }

    public Class<?> getJarClass() {
        return jarClass;
    }

    public Class<? extends Mapper> getMapperClass() {
        return mapperClass;
    }

    public Class<? extends Reducer> getReducerClass() {
        return reducerClass;
    }

    public Class<? extends Writable> getMapOutputKeyClass() {
        return mapOutputKeyClass;
    }

    public Class<? extends Writable> getMapOutputValueClass() {
        return mapOutputValueClass;
    }

    public Class<? extends Writable> getOutputKeyClass() {
        return outputKeyClass;
    }

    public Class<? extends Writable> getOutputValueClass() {
        return outputValueClass;
    }

    public Class<? extends Reducer> getCombinerClass() {
        return combinerClass;
    }

    public static Builder builder() {
        return new Builder();
    }

    private RunJobParam(String inputPath, String outputPath, String jobName,
                        Class<?> jarClass, Class<? extends Mapper> mapperClass,
                        Class<? extends Reducer> reducerClass,
                        Class<? extends Reducer> combinerClass,
                        Class<? extends Writable> mapOutputKeyClass,
                        Class<? extends Writable> mapOutputValueClass,
                        Class<? extends Writable> outputKeyClass,
                        Class<? extends Writable> outputValueClass,
                        Class<? extends Partitioner> partitionClass) {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.jobName = jobName;
        this.jarClass = jarClass;
        this.mapperClass = mapperClass;
        this.reducerClass = reducerClass;
        this.combinerClass = combinerClass;
        this.mapOutputKeyClass = mapOutputKeyClass;
        this.mapOutputValueClass = mapOutputValueClass;
        this.outputKeyClass = outputKeyClass;
        this.outputValueClass = outputValueClass;
        this.partitionClass = partitionClass;
    }

    public static class Builder {

        private String inputPath;

        private String outputPath;

        private String jobName;

        private Class<?> jarClass;

        private Class<? extends Mapper> mapperClass;

        private Class<? extends Reducer> reducerClass;

        private Class<? extends Reducer> combinerClass;

        private Class<? extends Writable> mapOutputKeyClass;

        private Class<? extends Writable> mapOutputValueClass;

        private Class<? extends Writable> outputKeyClass;

        private Class<? extends Writable> outputValueClass;

        private Class<? extends Partitioner> partitionClass;

        public Builder() {
        }

        public Builder inputPath(String inputPath) {
            this.inputPath = inputPath;
            return this;
        }

        public Builder outputPath(String outputPath) {
            this.outputPath = outputPath;
            return this;
        }

        public Builder jobName(String jobName) {
            this.jobName = jobName;
            return this;
        }

        public Builder jarClass(Class<?> jarClass) {
            this.jarClass = jarClass;
            return this;
        }

        public Builder mapperClass(Class<? extends Mapper> mapperClass) {
            this.mapperClass = mapperClass;
            return this;
        }

        public Builder reducerClass(Class<? extends Reducer> reducerClass) {
            this.reducerClass = reducerClass;
            return this;
        }

        public Builder combinerClass(Class<? extends Reducer> combinerClass) {
            this.combinerClass = combinerClass;
            return this;
        }

        public Builder mapOutputKeyClass(Class<? extends Writable> mapOutputKeyClass) {
            this.mapOutputKeyClass = mapOutputKeyClass;
            return this;
        }

        public Builder mapOutputValueClass(Class<? extends Writable> mapOutputValueClass) {
            this.mapOutputValueClass = mapOutputValueClass;
            return this;
        }

        public Builder outputKeyClass(Class<? extends Writable> outputKeyClass) {
            this.outputKeyClass = outputKeyClass;
            return this;
        }

        public Builder outputValueClass(Class<? extends Writable> outputValueClass) {
            this.outputValueClass = outputValueClass;
            return this;
        }

        public Builder partitionClass(Class<? extends Partitioner> partitionClass) {
            this.partitionClass = partitionClass;
            return this;
        }

        public RunJobParam build() {
            return new RunJobParam(inputPath, outputPath, jobName, jarClass, mapperClass, reducerClass, combinerClass,
                    mapOutputKeyClass, mapOutputValueClass, outputKeyClass, outputValueClass, partitionClass);
        }
    }
}
