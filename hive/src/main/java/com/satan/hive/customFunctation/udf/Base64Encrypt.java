package com.satan.hive.customFunctation.udf;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Objects;

/**
 * @author liuwenyi
 * @date 2020/12/10
 */
public class Base64Encrypt extends UDF {

    public String evaluate(String message) {
        if (StringUtils.isBlank(message)) {
            return "";
        }
        String newMessage = null;
        byte[] bt = message.getBytes();
        return null;
    }
}
