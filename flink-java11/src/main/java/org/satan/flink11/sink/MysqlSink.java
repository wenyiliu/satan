package org.satan.flink11.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.satan.flink11.entity.OrderResult;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author liuwenyi
 * @date 2022/09/11
 */
public class MysqlSink {

    public static class OrderResultMysqlSink extends RichSinkFunction<OrderResult> {
        Connection connection;
        PreparedStatement ps;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            connection = DriverManager.getConnection("jdbc:mysql://hadoop01:3306/test?useUnicode=true" +
                    "&characterEncoding=utf8&allowMultiQueries=true&serverTimezone" +
                    "=Asia/Shanghai", "root", "123456");
            ps = connection.prepareStatement("INSERT INTO order_result(name, type, amount, num) VALUES (?, ?, ?, ?)");
        }

        @Override
        public void close() throws Exception {
            if (connection != null) {
                connection.close();
            }
            if (ps != null) {
                ps.close();
            }
        }

        @Override
        public void invoke(OrderResult value, Context context) throws Exception {
            ps.setString(1, value.getName());
            ps.setInt(2, value.getType());
            ps.setBigDecimal(3, value.getAmount());
            ps.setInt(4, value.getNum());
            ps.execute();
        }
    }
}
