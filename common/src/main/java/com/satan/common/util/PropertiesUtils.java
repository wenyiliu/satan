package com.satan.common.util;

import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author liuwenyi
 * @date 2020/11/30
 */
public class PropertiesUtils {

    private static Properties PRO = new Properties();

    public PropertiesUtils(String fieldName) {
        InputStream is = PropertiesUtils.class.getClassLoader().getResourceAsStream(fieldName);
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
