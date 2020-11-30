package com.satan.hadoop.model.result;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author liuwenyi
 * @date 2020/11/25
 */
public class MinMax implements Writable {

    private int min;

    private int max;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(min);
        out.writeInt(max);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.min = in.readInt();
        this.max = in.readInt();
    }

    public MinMax() {
    }

    public MinMax(int min, int max) {
        this.min = min;
        this.max = max;
    }

    public int getMin() {
        return min;
    }

    public void setMin(int min) {
        this.min = min;
    }

    public int getMax() {
        return max;
    }

    public void setMax(int max) {
        this.max = max;
    }

    @Override
    public String toString() {
        return "min=" + min + "|max=" + max;
    }
}
