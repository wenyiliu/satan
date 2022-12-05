package com.satan.flink13.map;

import com.satan.flink13.entrty.Student;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

import java.time.LocalDateTime;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * @author liuwenyi
 * @date 2022/12/1
 **/
@Slf4j
public class TestMapFuncation extends RichMapFunction<String, String> {

    private static transient ScheduledExecutorService service;

    private static final long REFRESH_INTERVAL_SECONDS = 60;

    static {
        service = new ScheduledThreadPoolExecutor(2, (ThreadFactory) Thread::new);
    }

    @Override
    public String map(String value) throws Exception {
        return value;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println(System.currentTimeMillis());
        service.scheduleWithFixedDelay(() -> {
            System.out.println(LocalDateTime.now());

        }, 0, REFRESH_INTERVAL_SECONDS, TimeUnit.SECONDS);
    }

    @Override
    public void close() throws Exception {
        super.close();
    }
}
