package org.satan.flink11.job;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.util.keys.KeySelectorUtil;
import org.apache.flink.util.Collector;
import org.satan.flink11.entity.Order;
import org.satan.flink11.entity.OrderResult;
import org.satan.flink11.sink.MysqlSink;
import org.satan.flink11.source.KafkaSource;

import java.math.BigDecimal;

/**
 * @author liuwenyi
 * @date 2022/09/11
 */
public class OrderJob {

    public static void main(String[] args) throws Exception {
        final String topic = "order";
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(50000L);
        env.setStateBackend((StateBackend) new FsStateBackend("file:////Users/liuwenyi/IdeaProjects/satan/data/checkpoint_result/flink11/order"));

        // 获取 kafka 中订单数据
        DataStream<String> dataStream4Kafka = env.addSource(KafkaSource.getConsumer(topic));

        SingleOutputStreamOperator<Order> orderData2Map = dataStream4Kafka.map(
                new MapFunction<String, Order>() {
                    @Override
                    public Order map(String value) throws Exception {
                        return JSON.parseObject(value, Order.class);
                    }
                });

        KeyedStream<Order, Tuple2<String, Integer>> keyByNameAndType = orderData2Map.keyBy(
                new KeySelector<Order, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> getKey(Order value) throws Exception {
                        return Tuple2.of(value.getName(), value.getType());
                    }
                });

        WindowedStream<Order, Tuple2<String, Integer>, TimeWindow> orderDataWindowedTenSeconds
                = keyByNameAndType.timeWindow(Time.seconds(10));
        
        orderDataWindowedTenSeconds.aggregate(new AggregateFunction<Order, Object, Object>() {
        })
        env.execute("order job");

    }
}
