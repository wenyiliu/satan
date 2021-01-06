package com.satan.hive.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @author liuwenyi
 * @date 2020/12/02
 */
public class HiveJdbcUtils {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    //填写hive的IP，之前在配置文件中配置的IP

    private static String url = "jdbc:hive2://master:10000";

    private static Connection conn;

    private static PreparedStatement ps;

    private static ResultSet rs;

    static {
        try {
            Class.forName(driverName);
            conn = DriverManager.getConnection(url, "root", "123456");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static PreparedStatement prepare(Connection conn, String sql) {

        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return ps;
    }

    public static void getAll() {
        String sql = "show databases";
        try {
            ps = prepare(conn, sql);
            rs = ps.executeQuery();
            int columns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                for (int i = 1; i <= columns; i++) {
                    System.out.print(rs.getString(i));
                    System.out.print("\t\t");
                }
                System.out.println();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        getAll();
    }
}
