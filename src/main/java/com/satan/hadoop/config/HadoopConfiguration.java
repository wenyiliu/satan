package com.satan.hadoop.config;

import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.Properties;

/**
 * @author liuwenyi
 * @date 2020/11/10
 */
public class HadoopConfiguration {

    private static volatile Configuration configuration;

    private static final String HDFS_URL_NAME = "fs.defaultFS";
    private static final String DFS_CLIENT_USE_DATANODE_HOSTNAME = "dfs.client.use.datanode.hostname";


    private static final Properties PRO = new Properties();

    static {
        InputStream is = HadoopConfiguration.class.getClassLoader().getResourceAsStream("config.properties");
        try {
            PRO.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static org.apache.hadoop.conf.Configuration getConfiguration() {
        if (Objects.isNull(configuration)) {
            synchronized (HadoopConfiguration.class) {
                if (Objects.isNull(configuration)) {
                    configuration = new Configuration();
                    configuration.set(HDFS_URL_NAME, PRO.getProperty(HDFS_URL_NAME));
                    configuration.set(DFS_CLIENT_USE_DATANODE_HOSTNAME, PRO.getProperty(DFS_CLIENT_USE_DATANODE_HOSTNAME));
                    return configuration;
                }
            }
        }
        return configuration;
    }

    private HadoopConfiguration() {
    }
}
