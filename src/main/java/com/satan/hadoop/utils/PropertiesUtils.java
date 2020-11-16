package com.satan.hadoop.utils;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author liuwenyi
 * @date 2020/11/15
 */
public final class PropertiesUtils {

    private static final Properties PRO = new Properties();

    static {
        InputStream is = PropertiesUtils.class.getClassLoader().getResourceAsStream("config.properties");
        try {
            PRO.load(is);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String getValue(String key) {
        try {
            if (StringUtils.isBlank(key)) {
                throw new NullPointerException("key值为空");
            }
            return PRO.getProperty(key);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
