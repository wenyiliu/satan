package com.satan.hadoop.hadoopEnum;

/**
 * @author liuwenyi
 * @date 2020/11/17
 */
public enum ClusterCenterInitMethodEnum {

    /**
     * 聚类中心
     */
    INIT("init", "初始化"),
    CUSTOM("custom", "自定义"),
    KMEANS__("kmeans++", "kmeans++"),

    ;

    private String method;

    private String desc;

    public String getMethod() {
        return method;
    }

    public String getDesc() {
        return desc;
    }

    ClusterCenterInitMethodEnum(String method, String desc) {
        this.method = method;
        this.desc = desc;
    }
}
