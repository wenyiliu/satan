package com.satan.hadoop.model.result;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author liuwenyi
 * @date 2020/11/19
 */
public class UserItem implements WritableComparable<UserItem> {

    private int userId;

    private int itemId;

    private int score;

    private long timestamp;


    @Override
    public int compareTo(UserItem o) {
        int tmp = this.userId - o.userId;
        if (tmp == 0) {
            tmp = this.itemId - o.itemId;
        }
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(userId);
        out.writeInt(itemId);
        out.writeInt(score);
        out.writeLong(timestamp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.userId = in.readInt();
        this.itemId = in.readInt();
        this.score = in.readInt();
        this.timestamp = in.readLong();
    }
}
