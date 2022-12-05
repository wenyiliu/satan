package com.satan.flink.job;

import com.satan.flink.datastream.source.ClickSource;
import com.satan.flink.utils.EnvUtils;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;

/**
 * @author liuwenyi
 * @date 2022/9/26
 **/
public class StreamingFlinkSinkRowTest {
    public static void main(String[] args) throws Exception {
        String time = LocalDate.now() + " " + LocalTime.now();
        System.out.println(time);
//        System.setProperty("HADOOP_USER_NAME","root");
//        StreamExecutionEnvironment env = EnvUtils.getWebUIEnv();
//        DataStreamSource<String> dataStreamSource = env.addSource(new ClickSource());
//
//        DefaultRollingPolicy<String, String> policy = DefaultRollingPolicy.builder()
//                .withRolloverInterval(Duration.ofMinutes(10))
//                .withMaxPartSize(new MemorySize(1024L))
//                .build();
//
//        StreamingFileSink<String> sink = StreamingFileSink.forRowFormat(
//                        new Path("hdfs://hadoop01:9000/tmp/StreamingFileSink/row_format"),
//                        new SimpleStringEncoder<String>("UTF-8"))
//                .withRollingPolicy(policy)
//                .build();
//        dataStreamSource.addSink(sink);
//
//        env.execute();

    }
}
