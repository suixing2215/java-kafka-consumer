package com.alone.kafka.utils;

import com.alone.kafka.entry.Offset;

import java.io.IOException;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * JDBC操作工具类, 提供注册驱动, 连接, 发送器, 动态绑定参数, 关闭资源等方法
 * jdbc连接参数的提取, 使用Properties进行优化(软编码)
 *
 * 使用druid的连接池获取数据库连接
 */
public class DBUtils {

//    private static String driver;
//    private static String url;
//    private static String user;
//    private static String password;
//
//    static {
//        // 借助静态代码块保证配置文件只读取一次就行
//        // 创建Properties对象
//        Properties prop = new Properties();
//        try {
//            // 加载配置文件, 调用load()方法
//            // 类加载器加载资源时, 去固定的类路径下查找资源, 因此, 资源文件必须放到src目录才行
//            prop.load(DBUtils.class.getClassLoader().getResourceAsStream("db.properties"));
//            // 从配置文件中获取数据为成员变量赋值
//            driver = prop.getProperty("db.driver").trim();
//            url = prop.getProperty("db.url").trim();
//            user = prop.getProperty("db.user").trim();
//            password = prop.getProperty("db.password").trim();
//            // 加载驱动
//            Class.forName(driver);
//        } catch (IOException e) {
//            e.printStackTrace();
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
//    }

    /**
     * 动态绑定参数
     *
     * @param pstmt
     * @param params
     */
    public static void bindParam(PreparedStatement pstmt, Object... params) {
        try {
            for (int i = 0; i < params.length; i++) {
                pstmt.setObject(i + 1, params[i]);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    /**
     * 预处理发送器
     *
     * @param conn
     * @param sql
     * @return
     */
    public static PreparedStatement getPstmt(Connection conn, String sql) {
        PreparedStatement pstmt = null;
        try {
            pstmt = conn.prepareStatement(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return pstmt;
    }

    /**
     * 获取发送器的方法
     *
     * @param conn
     * @return
     */
    public static Statement getStmt(Connection conn) {
        Statement stmt = null;
        try {
            stmt = conn.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return stmt;
    }

    /**
     * 获取数据库连接的方法
     *
     * @return
     */
    public static Connection getConn() {
        Connection conn = null;
        try {
//            conn = DriverManager.getConnection(url, user, password);
            conn = DataSourceUtil.getConnection();
            return conn;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取特定消费者组，主题，分区下的偏移量
     *
     * @return offset
     */
    public static long queryOffset(String sql, Object... params) {
        Connection conn = getConn();
        long offset = 0;
//        String offset = "0";
        PreparedStatement preparedStatement = getPstmt(conn, sql);
        bindParam(preparedStatement, params);

        ResultSet resultSet = null;
        try {
            resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                offset = resultSet.getLong("sub_topic_partition_offset");
//                offset = resultSet.getString("untiloffset");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            close(resultSet, preparedStatement, conn);
        }

        return Long.valueOf(offset);
    }

    /**
     * 根据特定消费者组，主题，分区，批量更新偏移量
     *
     * @param offsets
     */
    public static void updateList(String sql, List<Offset> offsets) {
        PreparedStatement preparedStatement = null;
        Connection connection = null;
        try {
            connection = getConn();
//            int i = 0;
            for (Offset offset : offsets) {
//                System.out.println(i+"======================");
//                i += 1;
//                System.out.println(offset);
//                System.out.println("-------------------------------------------------------------------------------------");
                preparedStatement = getPstmt(connection, sql);
                bindParam(preparedStatement,
                        offset.getConsumerGroup(),
                        offset.getSubTopic(),
                        offset.getSubTopicPartitionId(),
                        offset.getSubTopicPartitionOffset(),
                        offset.getTimestamp()
                );
                try {
                    preparedStatement.executeUpdate();
                } catch (SQLException e) {
                    e.printStackTrace();
                    try {
                        connection.rollback();
                    } catch (SQLException exception) {
                        exception.printStackTrace();
                        System.out.println("数据回滚时间：" + DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now()));
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(null, preparedStatement, connection);
        }
    }

    /**
     * 根据特定消费者组，主题，分区，更新偏移量
     *
     * @param offset
     */
    public static void update(String sql, Offset offset) {
        Connection conn = getConn();
        PreparedStatement preparedStatement = getPstmt(conn, sql);

        bindParam(preparedStatement,
                offset.getConsumerGroup(),
                offset.getSubTopic(),
                offset.getSubTopicPartitionId(),
                offset.getSubTopicPartitionOffset(),
                offset.getTimestamp()
        );

        try {
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
            try {
                conn.rollback();
            } catch (SQLException exception) {
                exception.printStackTrace();
            }
        } finally {
            close(null, preparedStatement, conn);
        }

    }

    public static String buildInsertString(String tableName, List<String> columnKey) {
        StringBuffer columnSql = new StringBuffer("");
        StringBuffer unknownMarkSql = new StringBuffer("");
        StringBuffer sql = new StringBuffer("");
        for (int j = 0; j < columnKey.size(); j++) {
            String m = columnKey.get(j);
            columnSql.append(m);
            columnSql.append(",");

            unknownMarkSql.append("?");
            unknownMarkSql.append(",");
        }

        sql.append("INSERT INTO ");
        sql.append(tableName);
        sql.append(" (");
        sql.append(columnSql.substring(0, columnSql.length() - 1));
        sql.append(" )  VALUES (");
        sql.append(unknownMarkSql.substring(0, unknownMarkSql.length() - 1));
        sql.append(" )");
        return sql.toString();
    }

    public static void insertAllByList(String tableName, List<Map<String, Object>> dataList, List<String> cols)
            throws Exception {
        PreparedStatement preparedStatement = null;
        int c = 0;
        Connection connection = null;
        try {
            connection = getConn();
            String insertStr = buildInsertString(tableName, cols);
//            System.out.println(insertStr);
            preparedStatement = connection.prepareStatement(insertStr);
            connection.setAutoCommit(false);
            for (Map<String, Object> data : dataList) {
                for (int i = 0; i < cols.size(); i++) {
                    Object colValue = data.get(cols.get(i));
                    try {
                        preparedStatement.setObject(i + 1, colValue);
                    } catch (Exception e) {
                        preparedStatement.setTimestamp(i + 1, null);
                    }
                }
                preparedStatement.addBatch();
                c += 1;
            }
            preparedStatement.executeBatch();
            preparedStatement.clearBatch();
            connection.commit();
        } catch (SQLException e) {
            e.printStackTrace();
            connection.rollback();
            System.out.println("数据回滚时间：" + DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.now()));
//            String sqlState = e.getSQLState();
//            // 这个08S01就是这个异常的sql状态。单独处理手动重新链接就可以了。
//            if ("08S01".equals(sqlState) || "40001".equals(sqlState))
//            {
//                c--;
//            } else {
//                c = 0;
//            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new Exception("执行存入数据失败");
        } finally {
            close(null, preparedStatement, connection);
        }

    }


    /**
     * 统一关闭资源
     *
     * @param rs
     * @param stmt
     * @param conn
     */
    public static void close(ResultSet rs, PreparedStatement stmt, Connection conn) {
//        try {
//            if (rs != null) {
//                rs.close();
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        try {
//            if (stmt != null) {
//                stmt.close();
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        try {
//            if (conn != null) {
//                conn.close();
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
        DataSourceUtil.closeResource(rs,stmt,conn);
    }

}


