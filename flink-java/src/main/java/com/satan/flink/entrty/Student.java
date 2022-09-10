package com.satan.flink.entrty;

/**
 * @author liuwenyi
 * @date 2022/9/6
 **/

public class Student {

    private Integer id;

    private Integer age;

    private String name;

    private String address;

    private long timestamp;


    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getAge() {
        return age;
    }

    public void setAge(Integer age) {
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Student(Integer id, Integer age, String name, String address, long timestamp) {
        this.id = id;
        this.age = age;
        this.name = name;
        this.address = address;
        this.timestamp = timestamp;
    }

}
