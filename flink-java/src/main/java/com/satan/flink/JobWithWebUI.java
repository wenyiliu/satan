package com.satan.flink;

import com.satan.flink.datastream.source.ClickSource;
import com.satan.flink.utils.EnvUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liuwenyi
 * @date 2022/9/8
 **/
public class JobWithWebUI {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = EnvUtils.getWebUIEnv();
        DataStreamSource<String> datasource = env.addSource(new ClickSource());
        datasource.print();

        env.execute();

    }
}
