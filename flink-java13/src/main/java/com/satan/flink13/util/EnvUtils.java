package com.satan.flink13.util;

import lombok.Builder;
import lombok.Data;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * @author liuwenyi
 * @date 2022/11/30
 **/
public class EnvUtils {

    @Data
    @Builder
    public static class Env {

        private StreamExecutionEnvironment env;

        private SimpleDateFormat simpleDateFormat;
    }

    public static StreamExecutionEnvironment getStreamExecutionEnvironment(ParameterTool parameterTool) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        //注册全局变量
        env.getConfig().setGlobalJobParameters(parameterTool);

        StateBackend stateBackend = new FsStateBackend("/Users/liuwenyi/IdeaProjects/satan/data/flink13_ck", true);
//        MemoryStateBackend stateBackend = new MemoryStateBackend();
        env.setStateBackend(stateBackend);

        //checkpoint配置
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(2,
                org.apache.flink.api.common.time.Time.of(3, TimeUnit.SECONDS)));
        //设置任务关闭的时候保留最后一次CK数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //允许失败三次，因为初次启动全量扫描表比较耗时且没有checkpoint
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        return env;
    }

    public static Env getEnv(Integer i) {
        StreamExecutionEnvironment env =getWebUIEnv();
        if (Objects.isNull(i) || i < 1) {
            env.setParallelism(1);
        } else {
            env.setParallelism(i);
        }
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3,
                org.apache.flink.api.common.time.Time.of(3, TimeUnit.MINUTES)));
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(400000);
        env.getCheckpointConfig().setCheckpointTimeout(200000);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(6);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage("file:////Users/liuwenyi/IdeaProjects/satan/data/flink13_ck");

        return Env.builder().env(env).build();
    }

    public static StreamExecutionEnvironment getWebUIEnv() {
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8082);
        configuration.setString("hadoop.user.name", "root");
        return StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
    }
}
