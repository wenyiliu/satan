package com.satan.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author liuwenyi
 * @date 2022/9/8
 **/
public class JobWithWebUI {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT, 8088);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        DataStream<String> dataStream = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = dataStream.flatMap(
                (FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    for (String value : s.split(" ")) {
                        collector.collect(new Tuple2<>(value, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT));
//        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = streamOperator.keyBy((KeySelector<Tuple2<String, Integer>, String>)
//                        stringIntegerTuple2 -> stringIntegerTuple2.f0)
//                .timeWindow(Time.seconds(5))
//                .reduce((ReduceFunction<Tuple2<String, Integer>>)
//                        (stringIntegerTuple2, t1) -> Tuple2.of(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1))
//                .returns(Types.TUPLE(Types.STRING, Types.INT));
        streamOperator.print();
        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
