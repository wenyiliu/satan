package com.satan.hadoop.mr;

import com.satan.hadoop.mr.AvgScoreMapReduceJob;
import com.satan.hadoop.mr.WordCountMapReduceJob;

/**
 * @author liuwenyi
 * @date 2020/11/10
 */
public class Test {
    public static void main(String[] args) throws Exception {
        String inputPath = "/user/root/order.txt";
        String outputPath = "/user/root/order_result";
        if (args.length >= 2) {
            inputPath = args[0];
            outputPath = args[1];
        }
        WordCountMapReduceJob.runJob(inputPath, outputPath);
    }
}
