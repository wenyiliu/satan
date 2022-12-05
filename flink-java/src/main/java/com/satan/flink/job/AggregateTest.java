package com.satan.flink.job;

import com.alibaba.fastjson.JSON;
import com.satan.flink.datastream.source.ClickSource;
import com.satan.flink.entrty.Student;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author liuwenyi
 * @date 2022/9/25
 **/
public class AggregateTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.addSource(new ClickSource());

        SingleOutputStreamOperator<Student> studentMap = dataStream.map(
                new MapFunction<String, Student>() {
                    @Override
                    public Student map(String value) throws Exception {
                        return JSON.parseObject(value, Student.class);
                    }
                });
        SingleOutputStreamOperator<Student> studentOperator = studentMap.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Student>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<Student>() {
                            @Override
                            public long extractTimestamp(Student element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }));
        studentOperator.keyBy(
                        new KeySelector<Student, String>() {
                            @Override
                            public String getKey(Student value) throws Exception {
                                return value.getAddress();
                            }
                        }).window(TumblingEventTimeWindows.of(Time.seconds(5)))
                ;
    }


    public static class AddressCountAgg implements AggregateFunction<Student, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Student value, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long a, Long b) {
            return null;
        }
    }

    public static class AddressProcess extends ProcessWindowFunction<Long, Student, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<Long, Student, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<Student> out) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
        }
    }
}
