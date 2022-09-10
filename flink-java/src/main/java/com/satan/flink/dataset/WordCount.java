package com.satan.flink.dataset;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 * @author liuwenyi
 * @date 2022/7/27
 **/
public class WordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.readTextFile("/Users/liuwenyi/IdeaProjects/satan/data/word.txt");
        FlatMapOperator<String, Tuple2<String, Integer>> operator = dataSource
                .flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    String[] values = s.split(" ");
                    for (String value : values) {
                        collector.collect(new Tuple2<>(value, 1));
                    }
                }).returns(Types.TUPLE(Types.STRING, Types.INT));
        UnsortedGrouping<Tuple2<String, Integer>> unsortedGrouping = operator.groupBy(0);

        AggregateOperator<Tuple2<String, Integer>> result = unsortedGrouping.sum(1);

        result.print();
    }
}
