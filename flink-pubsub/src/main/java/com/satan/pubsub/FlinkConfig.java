package com.satan.pubsub;

import lombok.Data;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

/**
 * @author liuwenyi
 * @date 2022/11/19
 **/
@Data
public class FlinkConfig {

    private StreamExecutionEnvironment env;

    private SimpleDateFormat format;



}
