package com.satan.flink;

import com.alibaba.fastjson.JSON;
import com.satan.flink.entrty.Student;

import java.nio.charset.StandardCharsets;

/**
 * @author liuwenyi
 * @date 2022/7/27
 **/
public class Test {
    public static void main(String[] args) {
        Student student = new Student(1, 1, "a", "b", System.currentTimeMillis());
        String value = JSON.toJSONString(student);

        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);

        String s = new String(bytes, StandardCharsets.UTF_8);
        System.out.println(s);
        Student student1 = JSON.parseObject(s, Student.class);
        System.out.println(student1.getAddress());
    }
}
