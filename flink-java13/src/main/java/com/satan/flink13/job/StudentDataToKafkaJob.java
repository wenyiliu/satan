package com.satan.flink13.job;

import com.satan.flink13.map.TestMapFuncation;
import com.satan.flink13.source.CustomStudentSource;
import com.satan.flink13.util.EnvUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @author liuwenyi
 * @date 2022/11/30
 **/
public class StudentDataToKafkaJob {

    public static void main(String[] args) throws Exception {
        EnvUtils.Env env = EnvUtils.getEnv(1);
        DataStream<String> dataStream = env.getEnv().addSource(new CustomStudentSource());


//        dataStream.print();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "hadoop01:9092");

        // ./kafka-console-consumer.sh --bootstrap-server hadoop01:9092 --topic topic_01 --from-beginning
        // ./kafka-topics.sh --bootstrap-server hadoop01:9092 --topic topic_01 --describe

        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>("topic_01", new SimpleStringSchema(), properties);
        dataStream.addSink(producer);

        env.getEnv().execute();
    }
}
