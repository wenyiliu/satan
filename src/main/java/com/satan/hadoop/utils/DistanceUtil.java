package com.satan.hadoop.utils;

import org.apache.commons.lang3.math.NumberUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author liuwenyi
 * @date 2020/11/17
 */
public class DistanceUtil {

    /**
     * 欧式距离
     *
     * @param d1 double[]
     * @param d2 double[]
     * @return double
     */
    public static double euclideanDistance(String[] d1, String[] d2) {
        double distance = 0;
        if (d1.length == 0 || d2.length == 0) {
            return Double.MAX_VALUE;
        }
        int minLen = Math.min(d1.length, d2.length);
        for (int i = 0; i < minLen; i++) {
            distance += Math.pow((NumberUtils.toDouble(d1[i]) - NumberUtils.toDouble(d2[i])), 2);
        }
        return BigDecimal.valueOf(Math.sqrt(distance))
                .setScale(2, RoundingMode.UP)
                .doubleValue();
    }

    /**
     * 杰卡德距离
     *
     * @param d1 double[]
     * @param d2 double[]
     * @return double
     */
    public static double jackardDistance(String[] d1, String[] d2) {
        if (d1.length == 0 || d2.length == 0) {
            return Double.MAX_VALUE;
        }
        List<String> d1List = new ArrayList<>(d1.length << 1);
        Collections.addAll(d1List, d1);
        List<String> d2List = new ArrayList<>(d1.length << 1);
        Collections.addAll(d2List, d2);
        int comment = 0;
        for (String s1 : d1List) {
            if (d2List.contains(s1)) {
                comment++;
            }
        }
        return (double) comment / (d1.length + d2.length - comment);
    }

    /**
     * 余弦距离
     *
     * @param d1 向量1
     * @param d2 向量2
     * @return 距离
     */
    public static double cosDistance(String[] d1, String[] d2) {
        if (d1.length == 0 || d2.length == 0) {
            return Double.MAX_VALUE;
        }
        double molecular = 0;
        double denominator1 = 0;
        double denominator2 = 0;
        int minLen = Math.min(d1.length, d2.length);
        for (int i = 0; i < minLen; i++) {
            double v1 = NumberUtils.toDouble(d1[i]);
            double v2 = NumberUtils.toDouble(d2[i]);
            molecular += v1 * v2;
            denominator1 += Math.pow(v1, 2);
            denominator2 += Math.pow(v2, 2);
        }
        if (molecular == 0) {
            return Double.MAX_VALUE;
        }
        return molecular / Math.sqrt(denominator1 * denominator2);
    }

    public static double lossFunction(String[] d1, String[] d2) {
        if (d1.length == 0 || d2.length == 0) {
            return Double.MAX_VALUE;
        }
        int min = Math.min(d1.length, d2.length);
        double loss = 0.0;
        for (int i = 0; i < min; i++) {
            loss += Math.abs(Double.parseDouble(d1[i]) - Double.parseDouble(d2[i]));
        }
        return loss;
    }
}
