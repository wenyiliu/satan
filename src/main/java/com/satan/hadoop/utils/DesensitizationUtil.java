package com.satan.hadoop.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

/**
 * @author liuwenyi
 * @date 2020/11/10
 */
public class DesensitizationUtil {
    private static final Integer DEFAULT_INDEX = 1;

    private static final String DEFAULT_REPLACE_VALUE = "*";

    private static final Integer MOBILE_LENGTH = 11;

    public DesensitizationUtil() {
    }

    public static String desensitizationName(String fullName) {
        return desensitizationName(fullName, DEFAULT_INDEX, DEFAULT_REPLACE_VALUE);
    }

    public static String desensitizationName(String fullName, Integer index) {
        return desensitizationName(fullName, index, DEFAULT_REPLACE_VALUE);
    }

    public static String desensitizationName(String fullName, String replaceValue) {
        return desensitizationName(fullName, DEFAULT_INDEX, replaceValue);
    }

    /**
     * 姓名脱敏
     *
     * @param fullName     名称全称
     * @param index        开始位置 默认 1
     * @param replaceValue 替换值 默认 *
     * @return 脱敏
     */
    public static String desensitizationName(String fullName, Integer index, String replaceValue) {
        if (StringUtils.isBlank(fullName)) {
            return "";
        }
        if (index == null || index <= 0) {
            index = DEFAULT_INDEX;
        }
        String name = StringUtils.left(fullName, index);
        if (StringUtils.isBlank(replaceValue)) {
            replaceValue = DEFAULT_REPLACE_VALUE;
        }
        return StringUtils.rightPad(name, StringUtils.length(fullName), replaceValue);
    }

    public static String desensitizationMobile(String mobile, Integer start, Integer end, String replaceValue) throws Exception {
        if (StringUtils.isBlank(mobile)) {
            return "";
        }
        if (mobile.length() != MOBILE_LENGTH || !NumberUtils.isCreatable(mobile)) {
            throw new Exception("请输入合法手机号");
        }
        if (start == null || start <= 0) {
            start = 3;
        }
        if (end == null || end <= 0) {
            end = 4;
        }
        if (StringUtils.isBlank(replaceValue)) {
            replaceValue = DEFAULT_REPLACE_VALUE;
        }
        int middle = (MOBILE_LENGTH - start - end) < 0 ? 0 : (MOBILE_LENGTH - start - end);
        StringBuilder tempReplaceValue = new StringBuilder("$1");
        for (int i = 0; i < middle; i++) {
            tempReplaceValue.append(replaceValue);
        }
        tempReplaceValue.append("$2");
        String regex = "(\\d{" + start + "})\\d{" + middle + "}(\\d{" + end + "})";
        return mobile.replaceAll(regex, tempReplaceValue.toString());
    }

    /**
     * 手机号码前三后四脱敏
     *
     * @param mobile 手机号
     * @return 脱敏
     */
    public static String desensitizationMobile(String mobile) throws Exception {
        return desensitizationMobile(mobile, null, null, null);
    }

    public static String desensitizationMobile(String mobile, String replaceValue) throws Exception {
        return desensitizationMobile(mobile, null, null, replaceValue);
    }
}
