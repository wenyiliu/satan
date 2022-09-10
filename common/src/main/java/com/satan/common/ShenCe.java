package com.satan.common;

import com.sensorsdata.analytics.javasdk.SensorsAnalytics;
import com.sensorsdata.analytics.javasdk.consumer.ConcurrentLoggingConsumer;

/**
 * @author liuwenyi
 * @date 2022/4/14
 **/
public class ShenCe {

    public static void main(String[] args) {
        final SensorsAnalytics sa = new SensorsAnalytics(new ConcurrentLoggingConsumer("/data/sa/access.log"));

        SensorsAnalytics sass = new SensorsAnalytics(new ConcurrentLoggingConsumer("D:\\data\\service", "D:\\var\\sa.lock"));
    }
}
