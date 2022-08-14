package jm.task.core.jdbc.util;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class Util {
    private final static String URL = "jdbc:mysql://localhost:3306/my_schema";
    private final static String NAME = "root";
    private final static String PASS = "Callypso1703";

    public static Connection getConnection() {
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            Connection conn = DriverManager.getConnection(URL, NAME, PASS);
            conn.setAutoCommit(false);
            return conn;
        } catch (ClassNotFoundException | SQLException e) {
            System.err.println("Не удалось законнектиться с бд");
            return null;
        }
    }
}
