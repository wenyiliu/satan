package com.satan.flink.utils;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author liuwenyi
 * @date 2022/9/26
 **/
public class EnvUtils {

    public static StreamExecutionEnvironment getWebUIEnv() {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        configuration.setString("hadoop.user.name","root");
        return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
    }

    public static StreamExecutionEnvironment getEnv() {

        return StreamExecutionEnvironment.getExecutionEnvironment();
    }
}
