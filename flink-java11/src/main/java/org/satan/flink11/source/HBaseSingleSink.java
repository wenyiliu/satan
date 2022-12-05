package org.satan.flink11.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.satan.flink11.entity.DBSource;
import org.satan.flink11.entity.HBaseRowData;
import org.satan.flink11.mapper.DataSourceManager;

import java.util.Objects;

/**
 * @author liuwenyi
 * @date 2022/10/15
 **/
@Slf4j
public class HBaseSingleSink extends BaseRichSink<HBaseRowData> {

    private String key;

    private transient Connection connection;

    private transient TableName tableName;

    private transient Table table = null;

    public HBaseSingleSink(String key) {
        this.key = key;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        DBSource source = DataSourceManager.selectSourceByKey(this.key);
        org.apache.hadoop.conf.Configuration configuration = HBaseConfiguration.create();
        configuration.set(HConstants.ZOOKEEPER_QUORUM, source.getHost());
        configuration.set(HConstants.ZOOKEEPER_ZNODE_PARENT, source.getUser());
        configuration.set("hbase.client.retries.number", "3");

        connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        tableName = TableName.valueOf(source.getDatabase());
        if (StringUtils.isBlank(source.getDatabase()) || !admin.tableExists(tableName)) {
            String errMsg = " HBASE 表不存在，表名:" + source.getDatabase();
            log.error(errMsg);
            throw new RuntimeException(errMsg);
        }

    }

    @Override
    public void invoke(HBaseRowData value, Context context) throws Exception {
        if (StringUtils.isBlank(value.getRowkey())) {
            log.error("HBASE rowkey 不能为空,{}", value);
            return;
        }
        if (value.getRowMap() == null || value.getRowMap().isEmpty()) {
            log.error("数据不能为空,{}", value);
            return;
        }
        Table table = connection.getTable(tableName);
        byte[] bytes = Bytes.toBytes(value.getRowkey());
        Put put = new Put(bytes);
        value.getRowMap().forEach((key1, value1) -> {
            byte[] cf = Bytes.toBytes(key1);
            value1.forEach((k2, v2) -> put.addColumn(cf,
                    Bytes.toBytes(k2),
                    System.currentTimeMillis(),
                    Bytes.toBytes(v2.toString())));
        });
        table.put(put);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (Objects.nonNull(table)) {
            table.close();
        }
        if (Objects.nonNull(connection)) {
            connection.close();
        }
    }
}
