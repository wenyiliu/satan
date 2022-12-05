package org.satan.flink11.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.satan.flink11.entity.DBSource;
import org.satan.flink11.entity.ValueType;
import org.satan.flink11.mapper.DataSourceManager;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;


/**
 * @author liuwenyi
 * @date 2022/10/15
 **/
@Slf4j
public class BaseRichSink<T> extends RichSinkFunction<T> {


    @Override
    public void open(Configuration parameters) throws Exception {
    }

    @Override
    public void invoke(T t, SinkFunction.Context context) throws Exception {
    }

    @Override
    public void close() throws Exception {
    }

    public final void setValue(PreparedStatement ps, Integer index, ValueType valueType, Object value) throws SQLException {
        if (value == null || (valueType == ValueType.STRING && value.equals("")) || (valueType == ValueType.TEXT && value.equals(""))) {
            ps.setNull(index, Types.NULL);
        } else {
            switch (valueType) {
                case STRING:
                    ps.setString(index, (String) value);
                    break;
                case INT:
                    ps.setInt(index, (Integer) value);
                    break;
                case LONG:
                    ps.setLong(index, (Long) value);
                    break;
                case FLOAT:
                    ps.setFloat(index, (Float) value);
                    break;
                case DOUBLE:
                    ps.setDouble(index, (Double) value);
                    break;
                case DECIMAL:
                    ps.setBigDecimal(index, (BigDecimal) value);
                    break;
                case TEXT:
                    ps.setString(index, (String) value);
                    break;
            }
        }
    }

    public final Connection getConn(String key) {
        DBSource source = DataSourceManager.selectSourceByKey(key);
        return getConn(source.getHost(), source.getPort(), source.getDatabase(), source.getUser(), source.getPassword());
    }

    public final Connection getConn(String host, Integer port, String db, String username, String password) {
        return getConn("com.mysql.jdbc.Driver", host, port, db, username, password);
    }

    public final Connection getConn(String driver, String host, Integer port, String db, String username, String password) {
        String urlTmp = "jdbc:mysql://%s:%s/%s?useUnicode=true&characterEncoding=utf-8&autoReconnect=true&useSSL=false";
        String url = String.format(urlTmp, host, port, db);
        try {
            Class.forName(driver);
            return DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            log.error("Error Get Connection: " + e);
            return null;
        }
    }
}
