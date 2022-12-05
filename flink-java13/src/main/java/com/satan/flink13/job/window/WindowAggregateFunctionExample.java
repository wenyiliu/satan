package com.satan.flink13.job.window;

import com.alibaba.fastjson.JSON;
import com.satan.flink13.entrty.Student;
import com.satan.flink13.source.CustomStudentSource;
import com.satan.flink13.util.EnvUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.util.HashSet;
import java.util.Objects;

/**
 * @author liuwenyi
 * @date 2022/12/5
 **/
public class WindowAggregateFunctionExample {
    public static void main(String[] args) {
        EnvUtils.Env env = EnvUtils.getEnv(1);

//        env.getEnv().getConfig().setAutoWatermarkInterval(200);
//        Properties properties = new Properties();
//        properties.setProperty("bootstrap.servers", "hadoop01:9092");
//        properties.setProperty("group.id", "CountWindowJob");
//        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("topic_01", new SimpleStringSchema(), properties);
//        consumer.setStartFromEarliest();
        DataStream<Student> dataStream = env.getEnv().addSource(new CustomStudentSource())
                .map(new MapFunction<String, Student>() {
                    @Override
                    public Student map(String value) throws Exception {
                        if (StringUtils.isNotBlank(value)) {
                            return JSON.parseObject(value, Student.class);
                        }
                        return null;
                    }
                })
                .filter(new FilterFunction<Student>() {
                    @Override
                    public boolean filter(Student value) throws Exception {
                        return Objects.nonNull(value);
                    }
                }).assignTimestampsAndWatermarks(WatermarkStrategy.<Student>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Student>() {
                            @Override
                            public long extractTimestamp(Student element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }));
        dataStream.keyBy(new KeySelector<Student, String>() {
            @Override
            public String getKey(Student value) throws Exception {
                return value.getName();
            }
        });
    }

    public static class AvgTest implements AggregateFunction<Student, Tuple2<HashSet<String>, Long>, Double> {

        @Override
        public Tuple2<HashSet<String>, Long> createAccumulator() {
            return Tuple2.of(new HashSet<String>(), 0L);
        }

        @Override
        public Tuple2<HashSet<String>, Long> add(Student value, Tuple2<HashSet<String>, Long> accumulator) {
            accumulator.f0.add(value.getName());
            return Tuple2.of(accumulator.f0, accumulator.f1 + 1L);
        }

        @Override
        public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
            return (double) (accumulator.f1 / accumulator.f0.size());
        }

        @Override
        public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> a, Tuple2<HashSet<String>, Long> b) {
            return null;
        }
    }
}
