package com.satan.hadoop.model.result;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author liuwenyi
 * @date 2020/11/12
 */
public class UserLog implements Writable {

    private Integer target;

    private Long timestamp;

    private String deviceId;

    private String newsId;


    public Integer getTarget() {
        return target;
    }

    public void setTarget(Integer target) {
        this.target = target;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getNewsId() {
        return newsId;
    }

    public void setNewsId(String newsId) {
        this.newsId = newsId;
    }


    public UserLog() {
    }

    public UserLog(Integer target, Long timestamp, String deviceId, String newsId) {
        this.target = target;
        this.timestamp = timestamp;
        this.deviceId = deviceId;
        this.newsId = newsId;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.write(target);
        out.writeLong(timestamp);
        out.writeUTF(deviceId);
        out.writeUTF(newsId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        target = in.readInt();
        timestamp = in.readLong();
        deviceId = in.readUTF();
        newsId = in.readUTF();
    }
}
