package com.satan.hadoop.model.result;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author liuwenyi
 * @date 2020/11/15
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ConsumerBO implements Writable {

    private String phone;

    private String address;

    private String name;

    private Long consume;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(phone);
        out.writeUTF(address);
        out.writeUTF(name);
        out.writeLong(consume);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.phone = in.readUTF();
        this.address = in.readUTF();
        this.name = in.readUTF();
        this.consume = in.readLong();
    }

    @Override
    public String toString() {
        return "ConsumerBO{" +
                "phone='" + phone + '\'' +
                ", address='" + address + '\'' +
                ", name='" + name + '\'' +
                ", consume=" + consume +
                '}';
    }
}
