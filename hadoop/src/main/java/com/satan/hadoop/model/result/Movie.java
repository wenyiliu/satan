package com.satan.hadoop.model.result;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author liuwenyi
 * @date 2020/11/16
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Movie implements WritableComparable<Movie> {

    private String name;

    private Integer score;

    @Override
    public int compareTo(Movie o) {
        return o.score - this.score;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(name);
        out.writeInt(score);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.name = in.readUTF();
        this.score = in.readInt();
    }

}

