package com.satan.hadoop.model.param;

import lombok.Builder;
import lombok.Data;

/**
 * @author liuwenyi
 * @date 2020/11/18
 */
@Data
@Builder
public class KmeansParam {

    private int maxIter;

    private String clusterPath;

    private String inputPath;

    private String outPutPath;

    private double threshold;

    private int clusterNums;
}
