package com.satan.hadoop.config;

import org.apache.hadoop.conf.Configuration;

import java.util.Objects;

/**
 * @author liuwenyi
 * @date 2020/11/10
 */
public class HadoopConfiguration {

    private static volatile Configuration configuration;

    private static final String HDFS_URL = "hdfs://master:9000";

    private HadoopConfiguration() {
    }

    public static Configuration getConfiguration() {
        if (Objects.isNull(configuration)) {
            synchronized (HadoopConfiguration.class) {
                if (Objects.isNull(configuration)) {
                    configuration = new Configuration();
                    configuration.set("fs.defaultFS", HDFS_URL);
                    configuration.set("dfs.client.use.datanode.hostname", "true");
                    return configuration;
                }
            }
        }
        return configuration;
    }
}
