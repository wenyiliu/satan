package com.satan.hive.customFunctation.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author liuwenyi
 * @date 2020/12/22
 */
public class Wilson extends UDF {

    public double evaluate(int pv, int click) {
        if (pv == 0 || click == 0) {
            return 0.00;
        }
        double z = 1.96f;
        double p = 1.0f * click / pv;
        return (p + z * z / (2.f * pv) - z * Math.sqrt((p * (1.0f - p) + z * z / (4.0f * pv)) / pv)) / (1.0f + z * z / pv);

    }

    public static void main(String[] args) {
        Wilson wilson = new Wilson();
        double evaluate = wilson.evaluate(1000, 100);
        System.out.println(evaluate);
    }
}
