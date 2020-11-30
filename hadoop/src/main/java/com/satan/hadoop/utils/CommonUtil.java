package com.satan.hadoop.utils;

import com.satan.hadoop.constant.Constant;
import org.apache.commons.lang3.StringUtils;

/**
 * @author liuwenyi
 * @date 2020/11/09
 */
public class CommonUtil {

    private static final String PUNCTUATION = "\\pP";

    public static String replacePunctuation(String text) {
        if (StringUtils.isBlank(text)) {
            return text;
        }
        return text.replaceAll(PUNCTUATION, Constant.BLANK);
    }


    public static void main(String[] args) {
        String text = "aaa\"aa[am],";
        System.out.println(replacePunctuation(text));
    }
}
