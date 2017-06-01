package com.hive.client;


import java.sql.*;

import com.hive.util.HiveUtil;
import org.apache.log4j.Logger;

/**
 * Created by houlongbin on 2016/11/4.
 */
public class HiveJdbcCli {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String url = "jdbc:hive2://127.0.0.1:10001/default;auth=noSasl";
    private static String user = "";
    private static String password = "";
    private static String sql = "";
    private static ResultSet res;
    private static final Logger log = Logger.getLogger(HiveJdbcCli.class);

    public static void main(String[] args)throws Exception {
        String ip= "";
        String port= "";
        String dbname= "";
        String username= "";
        String password= "";
        String databasetype= "SQL Server2008";
        String table="Base.test";
        HiveUtil client= HiveUtil.getInstance();
    //    client.initTable(table,databasetype,ip,port,dbname,username,password);
        log.info(client.getMaxDefaultColumns(table,dbname,"createdtime"));
;

    }


//    private static void countData(Statement stmt, String tableName)
//            throws SQLException {
//        sql = "select count(1) from " + tableName;
//        System.out.println("Running:" + sql);
//        res = stmt.executeQuery(sql);
//        System.out.println("执行“regular hive query”运行结果:");
//        while (res.next()) {
//            System.out.println("count ------>" + res.getString(1));
//        }
//    }
//
//    private static void selectData(Statement stmt, String tableName)
//            throws SQLException {
//        sql = "select * from " + tableName;
//        System.out.println("Running:" + sql);
//        res = stmt.executeQuery(sql);
//        System.out.println("执行 select * query 运行结果:");
//        while (res.next()) {
//            System.out.println(res.getInt(1) + "\t" + res.getString(2));
//        }
//    }
//
//    private static void loadData(Statement stmt, String tableName)
//            throws SQLException {
//        String filepath = "/home/hadoop01/data";
//        sql = "load data local inpath '" + filepath + "' into table "
//                + tableName;
//        System.out.println("Running:" + sql);
//        res = stmt.executeQuery(sql);
//
//    }

    private static void describeTables(Statement stmt, String tableName)
            throws SQLException {
        sql = "describe " + tableName;
        System.out.println("Running:" + sql);
        res = stmt.executeQuery(sql);
        System.out.println("执行 describe table 运行结果:");
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }
    }
    private static void createTable(Statement stmt, String tableName)
            throws SQLException {
        sql = "create table "
                + tableName
                + " (key int, value string)  row format delimited fields terminated by '\t'";
        stmt.executeQuery(sql);
    }

//    private static String dropTable(Statement stmt) throws SQLException {
//        // 创建的表名
//        String tableName = "testtable";
//        sql = "drop table " + tableName;
//        stmt.executeQuery(sql);
//        return tableName;
//    }

    private static Connection getConn() throws ClassNotFoundException,
            SQLException {
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(url, user, password);
        return conn;
    }


}
