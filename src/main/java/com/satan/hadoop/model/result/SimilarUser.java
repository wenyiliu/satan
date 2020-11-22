package com.satan.hadoop.model.result;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author liuwenyi
 * @date 2020/11/19
 */
public class SimilarUser implements WritableComparable<SimilarUser> {

    private String user;

    private double similar;

    @Override
    public int compareTo(SimilarUser o) {
        return Double.compare(o.similar, this.similar);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(user);
        out.writeDouble(similar);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.user = in.readUTF();
        this.similar = in.readDouble();
    }

    @Override
    public String toString() {
        return user + "|" + similar;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public double getSimilar() {
        return similar;
    }

    public void setSimilar(double similar) {
        this.similar = similar;
    }
}
