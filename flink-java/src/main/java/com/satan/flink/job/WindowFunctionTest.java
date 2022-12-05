package com.satan.flink.job;

import com.alibaba.fastjson.JSON;
import com.satan.flink.datastream.source.ClickSource;
import com.satan.flink.entrty.Student;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Objects;

/**
 * @author liuwenyi
 * @date 2022/9/25
 **/
public class WindowFunctionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Student> streamOperator = dataStream
                .map(new MapFunction<String, Student>() {
                    @Override
                    public Student map(String value) throws Exception {
                        if (StringUtils.isBlank(value)) {
                            return null;
                        }
                        return JSON.parseObject(value, Student.class);
                    }
                })
                .filter(new FilterFunction<Student>() {
                    @Override
                    public boolean filter(Student value) throws Exception {
                        return Objects.nonNull(value);
                    }
                });
        SingleOutputStreamOperator<Student> singleOutputStreamOperator = streamOperator.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Student>forBoundedOutOfOrderness(Duration.ofMinutes(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Student>() {
                            @Override
                            public long extractTimestamp(Student element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }));
        SingleOutputStreamOperator<Tuple2<String, Long>> clickResult = singleOutputStreamOperator.map(
                new MapFunction<Student, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Student value) throws Exception {
                        return Tuple2.of(value.getName(), 1L);
                    }
                });
        SingleOutputStreamOperator<Tuple2<String, Long>> clickCount = clickResult.keyBy(
                        new KeySelector<Tuple2<String, Long>, String>() {
                            @Override
                            public String getKey(Tuple2<String, Long> value) throws Exception {
                                return value.f0;
                            }
                        }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                });
//        dataStream.print();
        clickCount.print();
        env.execute();
    }
}
