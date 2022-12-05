package org.satan.flink11.mapper;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.satan.flink11.entity.DBSource;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Classname DataSourceManager
 * @Date 2020/9/11 上午5:39
 * @Created by Zalex
 */
@Slf4j
public class DataSourceManager {

    private DataSourceManager() {
    }

    private static final String mybatis_config = "mybatis-config.xml";

    private static SqlSessionFactory sqlSessionFactory;

    static {
        try {
            InputStream inputStream = Resources.getResourceAsStream(mybatis_config);
            sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
            SqlSession session = sqlSessionFactory.openSession();
            if (session != null) {
                log.info("DBSource Session OK");
            } else {
                throw new IOException("DBSource Session Empty, Check Configuration: " + mybatis_config);
            }
        } catch (IOException e) {
            e.printStackTrace();
            log.error("Read " + mybatis_config + "cause error" + e.toString());
        }

    }

    public static SqlSession getSession() {
        return sqlSessionFactory.openSession();
    }

    public static DBSource selectSourceByKey(String key) {
        SqlSession session = getSession();
        DBSourceMapper mapper = session.getMapper(DBSourceMapper.class);
        DBSource source = mapper.selectByKey(key);
        session.close();
        return source;
    }

    public static DataSource dataSource(String key) {
        return dataSource(key, 1, 1, 60000, 1);
    }

    public static DataSource dataSource(String key, int maxAlive, int initialSize, int maxWait, int minIdle) {
        DBSource source = selectSourceByKey(key);
        Properties properties = new Properties();
        String url = String.format("jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=utf-8&autoReconnect=true&useSSL=false&serverTimezone=UTC", source.getHost(), source.getPort(), source.getDatabase());
        properties.setProperty("url", url);
        properties.setProperty("username", source.getUser());
        properties.setProperty("password", source.getPassword());
        properties.setProperty("filters", "stat");
        properties.setProperty("maxActive", String.valueOf(maxAlive));
        properties.setProperty("initialSize", String.valueOf(initialSize));
        properties.setProperty("maxWait", String.valueOf(maxWait));
        properties.setProperty("minIdle", String.valueOf(minIdle));
        properties.setProperty("timeBetweenEvictionRunsMillis", "60000");
        properties.setProperty("minEvictableIdleTimeMillis", "300000");
        properties.setProperty("testWhileIdle", "true");
        properties.setProperty("testOnBorrow", "false");
        properties.setProperty("testOnReturn", "false");
        properties.setProperty("poolPreparedStatements", "true");
        properties.setProperty("maxOpenPreparedStatements", "1");
        properties.setProperty("asyncInit", "true");
        DataSource dataSource = null;
        try {
            dataSource = DruidDataSourceFactory.createDataSource(properties);
        } catch (Exception e) {
            log.error(e.getMessage(), e.getCause());
        }
        return dataSource;
    }

}