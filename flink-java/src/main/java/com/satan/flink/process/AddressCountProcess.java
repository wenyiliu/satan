package com.satan.flink.process;

import com.satan.flink.entrty.Student;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author liuwenyi
 * @date 2022/9/25
 **/
public class AddressCountProcess extends ProcessWindowFunction<Student,String,Boolean, TimeWindow> {
    @Override
    public void process(Boolean aBoolean, ProcessWindowFunction<Student, String, Boolean, TimeWindow>.Context context, Iterable<Student> elements, Collector<String> out) throws Exception {

    }
}
