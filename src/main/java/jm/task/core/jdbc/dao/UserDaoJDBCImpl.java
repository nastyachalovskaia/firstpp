package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class UserDaoJDBCImpl implements UserDao {
    public UserDaoJDBCImpl() {

    }

    public void createUsersTable() {
        String SQL_CREATE = "CREATE TABLE USERS"
                + "("
                + " ID bigint auto_increment PRIMARY KEY,"
                + " NAME varchar(100) NOT NULL,"
                + " LASTNAME varchar(100) NOT NULL,"
                + " AGE tinyint"
                + ")";
        Connection conn = null;
        Statement statement = null;
        try {
            conn = Util.getConnection();
            statement = conn.createStatement();
            statement.execute(SQL_CREATE);
            conn.commit();
        } catch (SQLException e) {
            System.err.println("Не удалось создать таблицу.");
            try {
                conn.rollback();
            } catch (SQLException ex) {
                System.err.println("Ничего не удалось откатить.");
            }

        } finally {
            try {
                conn.close();
                statement.close();
            } catch (SQLException e) {
                System.err.println("Не удалось закрыть соединение с бд.");
            }
        }
    }

    public void dropUsersTable() {
        String SQL_DROP = "DROP TABLE USERS";

        Connection conn = null;
        Statement statement = null;
        try {
            conn = Util.getConnection();
            statement = conn.createStatement();
            statement.execute(SQL_DROP);
            conn.commit();
        } catch (SQLException e) {
            System.err.println("Не удалось удалить таблицу.");
            try {
                conn.rollback();
            } catch (SQLException ex) {
                System.err.println("Ничего не удалось откатить.");
            }

        } finally {
            try {
                conn.close();
                statement.close();
            } catch (SQLException e) {
                System.err.println("Не удалось закрыть соединение с бд.");
            }
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        String SQL_SAVEUSER = "INSERT INTO USERS (NAME, LASTNAME, AGE) VALUES (?, ?, ?)";

        Connection conn = null;
        PreparedStatement statement = null;
        try {
            conn = Util.getConnection();
            statement = conn.prepareStatement(SQL_SAVEUSER);
            statement.setString(1, name);
            statement.setString(2, lastName);
            statement.setByte(3, age);
            statement.execute();
            conn.commit();
        } catch (SQLException e) {
            System.err.println("Не удалось сохранить пользователя.");
            try {
                conn.rollback();
            } catch (SQLException ex) {
                System.err.println("Ничего не удалось откатить.");
            }

        } finally {
            try {
                conn.close();
                statement.close();
            } catch (SQLException e) {
                System.err.println("Не удалось закрыть соединение с бд.");
            }
        }
    }

    public void removeUserById(long id) {
        String SQL_REMOVEUSERSBYID = "DELETE FROM USERS WHERE ID = ?";

        Connection conn = null;
        PreparedStatement statement = null;
        try {
            conn = Util.getConnection();
            statement = conn.prepareStatement(SQL_REMOVEUSERSBYID);
            statement.setLong(1, id);
            statement.execute();
            conn.commit();
        } catch (SQLException e) {
            System.err.println("Не удалось удалить пользователя по айди.");
            try {
                conn.rollback();
            } catch (SQLException ex) {
                System.err.println("Ничего не удалось откатить.");
            }

        } finally {
            try {
                conn.close();
                statement.close();
            } catch (SQLException e) {
                System.err.println("Не удалось закрыть соединение с бд.");
            }
        }
    }

    public List<User> getAllUsers() {

        String SQL_GET_ALL_USERS = "select * from users";
        Connection conn = null;
        Statement statement = null;
        ResultSet rs = null;
        List <User> userList = new ArrayList<>();

        try {
            conn = Util.getConnection();
            statement = conn.createStatement();
            rs = statement.executeQuery(SQL_GET_ALL_USERS);

            while (rs.next()) {
                User user = new User();
                user.setId(rs.getLong(1));
                user.setName(rs.getString(2));
                user.setLastName(rs.getString(3));
                user.setAge(rs.getByte(4));
                userList.add(user);
            }

            conn.commit();
        } catch (SQLException e) {
            System.err.println("Не удалось вернуть таблицу.");
            try {
                conn.rollback();
            } catch (SQLException ex) {
                System.err.println("Ничего не удалось откатить.");
            }

        } finally {
            try {
                conn.close();
                statement.close();
            } catch (SQLException e) {
                System.err.println("Не удалось закрыть соединение с бд.");
            }
        }
        return userList;
    }

    public void cleanUsersTable() {

        String SQL_CLEANUSERSTABLE = "TRUNCATE TABLE USERS";

        Connection conn = null;
        Statement statement = null;
        try {
            conn = Util.getConnection();
            statement = conn.createStatement();
            statement.execute(SQL_CLEANUSERSTABLE);
            conn.commit();
        } catch (SQLException e) {
            System.err.println("Не удалось очистить таблицу.");
            try {
                conn.rollback();
            } catch (SQLException ex) {
                System.err.println("Ничего не удалось откатить.");
            }

        } finally {
            try {
                conn.close();
                statement.close();
            } catch (SQLException e) {
                System.err.println("Не удалось закрыть соединение с бд.");
            }
        }

    }
}
