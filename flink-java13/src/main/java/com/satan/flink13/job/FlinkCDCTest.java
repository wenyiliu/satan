package com.satan.flink13.job;

import com.satan.flink13.util.EnvUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.source.MySqlSourceBuilder;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;

/**
 * @author liuwenyi
 * @date 2022/11/30
 **/
public class FlinkCDCTest {
    public static void main(String[] args) {
        // 1. 获取执行环境
        EnvUtils.Env env = EnvUtils.getEnv(1);
        MySqlSource<String> build = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("gmall-210325-flink")
                .tableList("gmall-210325-flink.base_trademark")
                // 如果不添加该参数，则消费指定数据库中所有表的数据，如果指定，指定方式为db.table，必须带上库名
                .deserializer(new StringDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
    }
}
