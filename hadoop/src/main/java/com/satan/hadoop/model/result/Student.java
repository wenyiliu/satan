package com.satan.hadoop.model.result;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author liuwenyi
 * @date 2020/11/16
 */
public class Student implements Writable {

    private String name;

    private int chScore;

    private int enScore;

    private int mathScore;


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getChScore() {
        return chScore;
    }

    public void setChScore(int chScore) {
        this.chScore = chScore;
    }

    public int getEnScore() {
        return enScore;
    }

    public void setEnScore(int enScore) {
        this.enScore = enScore;
    }

    public int getMathScore() {
        return mathScore;
    }

    public void setMathScore(int mathScore) {
        this.mathScore = mathScore;
    }

    public Student() {
    }

    public Student(String name, int chScore, int enScore, int mathScore) {
        this.name = name;
        this.chScore = chScore;
        this.enScore = enScore;
        this.mathScore = mathScore;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(chScore);
        out.writeInt(enScore);
        out.writeInt(mathScore);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.chScore = in.readInt();
        this.enScore = in.readInt();
        this.mathScore = in.readInt();
    }

    @Override
    public String toString() {
        return "Student{" +
                "name='" + name +
                ", chScore=" + chScore +
                ", enScore=" + enScore +
                ", mathScore=" + mathScore +
                '}';
    }
}
