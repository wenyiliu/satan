package com.satan.hadoop.model.result;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author liuwenyi
 * @date 2020/11/16
 */
public class Company implements WritableComparable<Company> {

    private int quarterly;

    private String name;

    private float profit;

    public int getQuarterly() {
        return quarterly;
    }

    public void setQuarterly(int quarterly) {
        this.quarterly = quarterly;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public float getProfit() {
        return profit;
    }

    public void setProfit(float profit) {
        this.profit = profit;
    }

    public Company() {
    }

    public Company(int quarterly, String name, float profit) {
        this.quarterly = quarterly;
        this.name = name;
        this.profit = profit;
    }

    @Override
    public int compareTo(Company o) {
        int tmp = (int) (o.profit - this.profit);
        if (tmp == 0) {
            tmp = o.quarterly - this.quarterly;
        }
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(quarterly);
        out.writeUTF(name);
        out.writeFloat(profit);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.quarterly = in.readInt();
        this.name = in.readUTF();
        this.profit = in.readFloat();
    }

    @Override
    public String toString() {
        return name + " 第 " + quarterly + " 的利润为：" + profit;
    }
}
