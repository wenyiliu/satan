package com.satan.flink13.job.window;

import com.alibaba.fastjson.JSON;
import com.satan.flink13.entrty.Student;
import com.satan.flink13.source.CustomStudentSource;
import com.satan.flink13.util.EnvUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Objects;

/**
 * @author liuwenyi
 * @date 2022/12/2
 **/
public class CountWindowJob {
    public static void main(String[] args) throws Exception {
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

        dataStream.map(new MapFunction<Student, Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> map(Student value) throws Exception {
                        return Tuple2.of(value.getAge(), 1L);
                    }
                }).keyBy(new KeySelector<Tuple2<Integer, Long>, Integer>() {
                    @Override
                    public Integer getKey(Tuple2<Integer, Long> value) throws Exception {
                        return value.f0;
                    }
                }).window(TumblingEventTimeWindows.of(Time.seconds(60)))
                .reduce(new ReduceFunction<Tuple2<Integer, Long>>() {
                    @Override
                    public Tuple2<Integer, Long> reduce(Tuple2<Integer, Long> value1, Tuple2<Integer, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                }).print()
        ;
        env.getEnv().execute();
    }
}
