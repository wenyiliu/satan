package org.satan.flink11.job;


import com.google.gson.Gson;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/**
 * @author liuwenyi
 * @date 2022/10/17
 **/
public class CoGroupTestJob {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //watermark 自动添加水印调度时间
        //env.getConfig().setAutoWatermarkInterval(200);

        List<Tuple3<String, String, Integer>> tuple3List1 = Arrays.asList(
                new Tuple3<>("李四", "girl", 28),
                new Tuple3<>("张三", "man", 9),
                new Tuple3<>("李四", "girl", 122),
                new Tuple3<>("赵六", "aa", 10)
        );
        List<Tuple2<String, String>> tuple3List2 = Arrays.asList(
                new Tuple2<>("伍七", "girl"),
                new Tuple2<>("伍七b", "girl"),
                new Tuple2<>("吴八", "man"),
                new Tuple2<>("吴八a", "man")
        );
        //Datastream 1
        DataStream<Tuple3<String, String, Integer>> dataStream1 = env.fromCollection(tuple3List1)
                //添加水印窗口,如果不添加，则时间窗口会一直等待水印事件时间，不会执行apply
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple3<String, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner((element, timestamp) -> System.currentTimeMillis()));
        //Datastream 2
        DataStream<Tuple2<String, String>> dataStream2 = env.fromCollection(tuple3List2)
                //添加水印窗口,如果不添加，则时间窗口会一直等待水印事件时间，不会执行apply
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<Tuple2<String, String>>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, String>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, String> element, long timestamp) {
                                return System.currentTimeMillis();
                            }
                        })
                );

        //对dataStream1和dataStream2两个数据流进行关联，没有关联也保留
        //Datastream 3
        DataStream<String> newDataStream = dataStream1.coGroup(dataStream2)
                .where(new KeySelector<Tuple3<String, String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple3<String, String, Integer> value) throws Exception {
                        return value.f1;
                    }
                })
                .equalTo(t3 -> t3.f1)
                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .apply(new CoGroupFunction<Tuple3<String, String, Integer>, Tuple2<String, String>, String>() {
                    @Override
                    public void coGroup(Iterable<Tuple3<String, String, Integer>> first, Iterable<Tuple2<String, String>> second, Collector<String> out) throws Exception {
                        StringBuilder sb = new StringBuilder();
                        Gson gson = new Gson();
                        //datastream1的数据流集合
                        for (Tuple3<String, String, Integer> tuple3 : first) {
                            sb.append(gson.toJson(tuple3)).append("11111  \n");
                        }
                        System.out.println("000000000000");
                        //datastream2的数据流集合
                        for (Tuple2<String, String> tuple3 : second) {
                            sb.append(gson.toJson(tuple3)).append("22222 \n");
                        }
                        out.collect(sb.toString());
                    }
                });
        newDataStream.print();
        env.execute("flink CoGroup job");
    }


}
