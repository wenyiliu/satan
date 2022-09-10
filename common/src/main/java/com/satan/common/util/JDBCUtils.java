package com.satan.common.util;

import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * @author liuwenyi
 * @date 2022/5/31
 **/
public class JDBCUtils {


    private static String url;
    private static String user;
    private static String password;
    private static String driver;

    static {
        Properties pro = new Properties();
        //这里是通过类加载器获取jdbc.propertise的绝对路径
        //首先获取类的加载器，然后通过类的加载器获取src路径下资源的绝对路径
        //这里的意思是不管模块如何移植，只要在模块当中，就能通过相对路径找到
        //绝对路径
        ClassLoader loader = JDBCUtils.class.getClassLoader();
        //通过类加载器获取scr路径下的资源的绝对路径
        URL res = loader.getResource("JDBC.properties");
        //获取绝对路径
        String path = res.getPath();

        try {
            pro.load(new FileReader(path));
        } catch (IOException e) {
            e.printStackTrace();
        }
        url = pro.getProperty("url");
        user = pro.getProperty("user");
        password = pro.getProperty("password");
        driver = pro.getProperty("driver");
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(url, user, password);
    }

    public static void close(Statement stmt, Connection conn) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void close(ResultSet rs, Statement stmt, Connection conn) {

        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
