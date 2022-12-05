package org.satan.flink11.entity;

import lombok.Data;

import java.sql.Timestamp;

/**
 * @Classname DBSource
 * @Date 2020/11/23 下午5:07
 * @Created by Zalex
 */
@Data
public class DBSource {

    private Integer id;
    private Timestamp date_create;
    private Timestamp date_update;
    private String key;
    private String type;
    private String host;
    private Integer port;
    private String database;
    private String user;
    private String password;
    private Integer readonly;
    private String comment;
    private String owner;
}
