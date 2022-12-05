package com.satan.flink13.entrty;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author liuwenyi
 * @date 2022/9/6
 **/
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Student {

    private Integer id;

    private Integer age;

    private String name;

    private String address;

    private long timestamp;

}
