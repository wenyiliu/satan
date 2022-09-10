/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.satan.flink11;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamingJob {

    public static void main(String[] args) throws Exception {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> dataStream = env.socketTextStream("localhost", 7777);

        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = dataStream.flatMap(
                (FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    for (String value : s.split(" ")) {
                        collector.collect(new Tuple2<>(value, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT));
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduce = streamOperator.keyBy((KeySelector<Tuple2<String, Integer>, String>)
                        stringIntegerTuple2 -> stringIntegerTuple2.f0)
                .timeWindow(Time.seconds(5))
                .reduce((ReduceFunction<Tuple2<String, Integer>>)
                        (stringIntegerTuple2, t1) -> Tuple2.of(stringIntegerTuple2.f0, stringIntegerTuple2.f1 + t1.f1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));
        reduce.print();
        // execute program
        env.execute("Flink Streaming Java API Skeleton");
    }
}
