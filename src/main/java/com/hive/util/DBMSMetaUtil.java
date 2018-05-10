package com.hive.util;

import com.alibaba.fastjson.JSONArray;

import java.sql.*;
import java.util.*;

import static com.hive.util.Constant.POST_JDBC;

/**
 * Created by houlongbin on 2016/11/7.
 */
public class DBMSMetaUtil {
    /**
     * 数据库类型,枚举
     *
     */
    public static enum DATABASETYPE {
        ORACLE, MYSQL, SQLSERVER, SQLSERVER2005, DB2, HIVE,INFORMIX, SYBASE, OTHER, EMPTY
    }

    /**
     * 根据字符串,判断数据库类型
     *
     * @param databasetype
     * @return
     */
    public static DATABASETYPE parseDATABASETYPE(String databasetype) {
        // 空类型
        if (null == databasetype || databasetype.trim().length() < 1) {
            return DATABASETYPE.EMPTY;
        }
        // 截断首尾空格,转换为大写
        databasetype = databasetype.trim().toUpperCase();
        // Oracle数据库
        if (databasetype.contains("ORACLE")) {
            //
            return DATABASETYPE.ORACLE;
        }
        // MYSQL 数据库
        if (databasetype.contains("MYSQL")) {
            //
            return DATABASETYPE.MYSQL;
        }
        // SQL SERVER 数据库
        if (databasetype.contains("SQL") && databasetype.contains("SERVER")) {
            //
            if (databasetype.contains("2005") || databasetype.contains("2008") || databasetype.contains("2012")) {

                try {
                    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }

                return DATABASETYPE.SQLSERVER2005;
            } else {
                try {
                    // 注册 JTDS
                    Class.forName("net.sourceforge.jtds.jdbc.Driver");
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                return DATABASETYPE.SQLSERVER;
            }
        }
        if (databasetype.contains("HIVE")) {
            try {
                // 注册 hive
                Class.forName("org.apache.hive.jdbc.HiveDriver");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return DATABASETYPE.HIVE;
        }
        // 下面的这几个没有经过实践测试, 判断可能不准确
        // DB2 数据库
        if (databasetype.contains("DB2")) {
            //
            return DATABASETYPE.DB2;
        }
        // INFORMIX 数据库
        if (databasetype.contains("INFORMIX")) {
            //
            return DATABASETYPE.INFORMIX;
        }
        // SYBASE 数据库
        if (databasetype.contains("SYBASE")) {
            //
            return DATABASETYPE.SYBASE;
        }

        // 默认,返回其他
        return DATABASETYPE.OTHER;
    }

    /**
     * 获取jdbc driver
     * @param dbtype
     * @return
     */
    public static String getJDBCDrive(DATABASETYPE dbtype){
        String returnDrive=null;
        if(dbtype.equals(DATABASETYPE.SQLSERVER2005)){
            returnDrive="com.microsoft.sqlserver.jdbc.SQLServerDriver";
        }else if(dbtype.equals(DATABASETYPE.MYSQL)){
            returnDrive="com.mysql.jdbc.Driver";
        }else if(dbtype.equals(DATABASETYPE.HIVE)){
            returnDrive="org.apache.hive.jdbc.HiveDriver";
        }else if(dbtype.equals(DATABASETYPE.ORACLE)){
            returnDrive="oracle.jdbc.driver.OracleDriver";
        }else if(dbtype.equals(DATABASETYPE.SQLSERVER)){
            returnDrive="net.sourceforge.jtds.jdbc.Driver";
        }
        return returnDrive;
    }
    /**
     * 列出数据库的所有表
     */
    // 可以参考: http://www.cnblogs.com/chinafine/articles/1847205.html
    public static List<Map<String, Object>> listTables(String databasetype, String ip, String port, String dbname,
                                                       String username, String password) {
        // 去除首尾空格
        databasetype = trim(databasetype);
        ip = trim(ip);
        port = trim(port);
        dbname = trim(dbname);
        username = trim(username);
        password = trim(password);
        //
        DATABASETYPE dbtype = parseDATABASETYPE(databasetype);
        //
        List<Map<String, Object>> result = null;
        String url = concatDBURL(dbtype, ip, port, dbname);
        Connection conn = getConnection(url, username, password);
        // Statement stmt = null;
        ResultSet rs = null;
        //
        try {
            // 这句话我也不懂是什么意思... 好像没什么用
            conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            // 获取Meta信息对象
            DatabaseMetaData meta = conn.getMetaData();
            // 数据库
            String catalog = null;
            // 数据库的用户
            String schemaPattern = null;// meta.getUserName();
            // 表名
            String tableNamePattern = null;//
            // types指的是table、view
            String[] types = { "TABLE" };
            // Oracle
            if (DATABASETYPE.ORACLE.equals(dbtype)) {
                schemaPattern = username;
                if (null != schemaPattern) {
                    schemaPattern = schemaPattern.toUpperCase();
                }
                // 查询
                rs = meta.getTables(catalog, schemaPattern, tableNamePattern, types);
            } else if (DATABASETYPE.MYSQL.equals(dbtype)) {
                // Mysql查询
                // MySQL 的 table 这一级别查询不到备注信息
                schemaPattern = dbname;
                rs = meta.getTables(catalog, schemaPattern, tableNamePattern, types);
            }  else if (DATABASETYPE.SQLSERVER.equals(dbtype) || DATABASETYPE.SQLSERVER2005.equals(dbtype)) {
                // SqlServer
                tableNamePattern = "%";
                rs = meta.getTables(catalog, schemaPattern, tableNamePattern, types);
            }  else if (DATABASETYPE.DB2.equals(dbtype)) {
                // DB2查询
                schemaPattern = "jence_user";
                tableNamePattern = "%";
                rs = meta.getTables(catalog, schemaPattern, tableNamePattern, types);
            } else if (DATABASETYPE.INFORMIX.equals(dbtype)) {
                // SqlServer
                tableNamePattern = "%";
                rs = meta.getTables(catalog, schemaPattern, tableNamePattern, types);
            } else if (DATABASETYPE.SYBASE.equals(dbtype)) {
                // SqlServer
                tableNamePattern = "%";
                rs = meta.getTables(catalog, schemaPattern, tableNamePattern, types);
            }  else {
                throw new RuntimeException("不认识的数据库类型!");
            }
            //
            result = parseResultSetToMapList(rs);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            close(rs);
            close(conn);
        }
        //
        return result;
    }

    /**
     * 列出表的所有字段
     */
    public static List<Map<String, Object>> listColumns(String databasetype, String ip, String port, String dbname,
                                                        String username, String password, String tableName) {
        // 去除首尾空格
        databasetype = trim(databasetype);
        ip = trim(ip);
        port = trim(port);
        dbname = trim(dbname);
        username = trim(username);
        password = trim(password);
        tableName = trim(tableName);
        //
        DATABASETYPE dbtype = parseDATABASETYPE(databasetype);
        //
        List<Map<String, Object>> result = null;
        String url = concatDBURL(dbtype, ip, port, dbname);
        Connection conn = getConnection(url, username, password);
        // Statement stmt = null;
        ResultSet rs = null;
        //
        try {
            // 获取Meta信息对象
            DatabaseMetaData meta = conn.getMetaData();
            // 数据库
            String catalog = null;
            // 数据库的用户
            String schemaPattern = null;// meta.getUserName();
            // 表名
            String tableNamePattern = tableName;//
            // 转换为大写
            if (null != tableNamePattern) {
                tableNamePattern = tableNamePattern.toUpperCase();
            }
            //
            String columnNamePattern = null;
            // Oracle
            if (DATABASETYPE.ORACLE.equals(dbtype)) {
                if(tableName.contains(".")){
                    // 查询
                    String[] schemaTable=tableName.split("\\.");
                    schemaPattern=schemaTable[0];
                    tableNamePattern=schemaTable[1].toUpperCase();
                }else{
                    schemaPattern = username;
                    if (null != schemaPattern) {
                        schemaPattern = schemaPattern.toUpperCase();
                    }
                }


            } else if(DATABASETYPE.SQLSERVER2005.equals(dbtype)) {
                //mssql分割模式与表名
                String[] schemaTable=tableName.split("\\.");
                schemaPattern=schemaTable[0];
                tableNamePattern=schemaTable[1].toUpperCase();

            }
//            schemaPattern="Base";
            rs = meta.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
            // TODO 获取主键列,但还没使用
            meta.getPrimaryKeys(catalog, schemaPattern, tableNamePattern);
            //
            result = parseResultSetToMapList(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            close(rs);
            close(conn);
        }
        //
        return result;
    }
    /**
     * 获取ResultSetMetaData
     */
    public static ResultSetMetaData  listColumnsMetaData(String databasetype, String ip, String port, String dbname,
                                                        String username, String password, String tableName) {
        // 去除首尾空格
        databasetype = trim(databasetype);
        ip = trim(ip);
        port = trim(port);
        dbname = trim(dbname);
        username = trim(username);
        password = trim(password);
        tableName = trim(tableName);
        //
        DATABASETYPE dbtype = parseDATABASETYPE(databasetype);
        //
        List<Map<String, Object>> result = null;
        String url = concatDBURL(dbtype, ip, port, dbname);
        Connection conn = getConnection(url, username, password);
        ResultSet rs = null;
        //
        try {
            // 获取Meta信息对象
            DatabaseMetaData meta = conn.getMetaData();
            // 数据库
            String catalog = null;
            // 数据库的用户
            String schemaPattern = null;// meta.getUserName();
            // 表名
            String tableNamePattern = tableName;//
            // 转换为大写
            if (null != tableNamePattern) {
                tableNamePattern = tableNamePattern.toUpperCase();
            }
            //
            String columnNamePattern = null;
            // Oracle
            if (DATABASETYPE.ORACLE.equals(dbtype)) {
                if(tableName.contains(".")){
                    // 查询
                    String[] schemaTable=tableName.split("\\.");
                    schemaPattern=schemaTable[0];
                    tableNamePattern=schemaTable[1].toUpperCase();
                }else{
                    schemaPattern = username;
                    if (null != schemaPattern) {
                        schemaPattern = schemaPattern.toUpperCase();
                    }
                }


            } else if(DATABASETYPE.SQLSERVER2005.equals(dbtype)) {
                //mssql分割模式与表名
                String[] schemaTable=tableName.split("\\.");
                schemaPattern=schemaTable[0];
                tableNamePattern=schemaTable[1].toUpperCase();

            }
            rs = meta.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
            return rs.getMetaData();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            close(rs);
            close(conn);
        }
        return null;
    }
    /**
     * 列出表的所有字段
     */
    public static ResultSet listColumnsRS(String databasetype, String ip, String port, String dbname,
                                                        String username, String password, String tableName) {
        // 去除首尾空格
        databasetype = trim(databasetype);
        ip = trim(ip);
        port = trim(port);
        dbname = trim(dbname);
        username = trim(username);
        password = trim(password);
        tableName = trim(tableName);
        //
        DATABASETYPE dbtype = parseDATABASETYPE(databasetype);
        //
        List<Map<String, Object>> result = null;
        String url = concatDBURL(dbtype, ip, port, dbname);
        Connection conn = getConnection(url, username, password);
        // Statement stmt = null;
        ResultSet rs = null;
        //
        try {
            // 获取Meta信息对象
            DatabaseMetaData meta = conn.getMetaData();
            // 数据库
            String catalog = null;
            // 数据库的用户
            String schemaPattern = null;// meta.getUserName();
            // 表名
            String tableNamePattern = tableName;//
            // 转换为大写
            if (null != tableNamePattern) {
                tableNamePattern = tableNamePattern.toUpperCase();
            }
            //
            String columnNamePattern = null;
            // Oracle
            if (DATABASETYPE.ORACLE.equals(dbtype)) {
                if(tableName.contains(".")){
                    // 查询
                    String[] schemaTable=tableName.split("\\.");
                    schemaPattern=schemaTable[0];
                    tableNamePattern=schemaTable[1].toUpperCase();
                }else{
                    schemaPattern = username;
                    if (null != schemaPattern) {
                        schemaPattern = schemaPattern.toUpperCase();
                    }
                }


            } else if(DATABASETYPE.SQLSERVER2005.equals(dbtype)) {
                //mssql分割模式与表名
                String[] schemaTable=tableName.split("\\.");
                schemaPattern=schemaTable[0];
                tableNamePattern=schemaTable[1].toUpperCase();

            }
//            schemaPattern="Base";
            rs = meta.getColumns(catalog, schemaPattern, tableNamePattern, columnNamePattern);
            return rs;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
//            close(rs);
            close(conn);
        }
        //
        return null;
    }

    /**
     * 执行sql
     * @param databasetype 数据库类型
     * @param ip
     * @param port
     * @param dbname
     * @param username
     * @param password
     * @param sql
     * @return
     */
    public static List<Map<String, Object>> executeSql(String databasetype, String ip, String port, String dbname,
                             String username, String password,String sql){
        // 去除首尾空格
        databasetype = trim(databasetype);
        ip = trim(ip);
        port = trim(port);
        dbname = trim(dbname);
        username = trim(username);
        password = trim(password);
        //
        DATABASETYPE dbtype = parseDATABASETYPE(databasetype);
        //
        List<Map<String, Object>> result = null;
        String url = concatDBURL(dbtype, ip, port, dbname);
        Connection conn = getConnection(url, username, password);
        ResultSet rs = null;
        Statement stmt = null;

        try {
            stmt =conn.createStatement();
            stmt.executeUpdate(sql);
            result = parseResultSetToMapList(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            close(rs);
            close(conn);
        }


        return result;


    }

    /**
     * 执行sql
     * @param databasetype 数据库类型
     * @param ip
     * @param port
     * @param dbname
     * @param username
     * @param password
     * @param sql
     * @return
     */
    public static List<Map<String, Object>> executeQuerySql(String databasetype, String ip, String port, String dbname,
                                                       String username, String password,String sql){
        // 去除首尾空格
        databasetype = trim(databasetype);
        ip = trim(ip);
        port = trim(port);
        dbname = trim(dbname);
        username = trim(username);
        password = trim(password);
        //
        DATABASETYPE dbtype = parseDATABASETYPE(databasetype);
        //
        List<Map<String, Object>> result = null;
        String url = concatDBURL(dbtype, ip, port, dbname);
        Connection conn = getConnection(url, username, password);
        ResultSet rs = null;
        Statement stmt = null;

        try {
            stmt =conn.createStatement();
            rs=stmt.executeQuery(sql);
            result = parseResultSetToMapList(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭资源
            close(rs);
            close(conn);
        }


        return result;


    }

    /**
     * 表是否存在
     * @param databasetype
     * @param ip
     * @param port
     * @param dbname
     * @param username
     * @param password
     * @param tableName
     * @return
     */
    public static  boolean validateTableExist(String databasetype, String ip, String port, String dbname,
                                      String username, String password,String tableName){
        boolean flag = false;
        // 去除首尾空格
        databasetype = trim(databasetype);
        ip = trim(ip);
        port = trim(port);
        dbname = trim(dbname);
        username = trim(username);
        password = trim(password);
        tableName = trim(tableName);
        //
        DATABASETYPE dbtype = parseDATABASETYPE(databasetype);
        ResultSet rs = null;
        try {
            String url = concatDBURL(dbtype, ip, port, dbname);
            Connection conn = getConnection(url, username, password);
            DatabaseMetaData meta = conn.getMetaData();
            String type [] = {"TABLE"};
            rs = meta.getTables(null, null, tableName, type);
            flag = rs.next();
            close(conn);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return flag;
    }
    /**
     * 根据IP,端口,以及数据库名字,拼接Oracle连接字符串
     *
     * @param ip
     * @param port
     * @param dbname
     * @return
     */
    public static String concatDBURL(DATABASETYPE dbtype, String ip, String port, String dbname) {
        //
        String url = "";
        // Oracle数据库
        if (DATABASETYPE.ORACLE.equals(dbtype)) {
            //
            url += "jdbc:oracle:thin:@";
            url += ip.trim();
            url += ":" + port.trim();
            url += ":" + dbname;

            // 如果需要采用 hotbackup
            String url2 = "";
            url2 = url2+"jdbc:oracle:thin:@(DESCRIPTION = (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)(HOST = "
                    + ip.trim() +")(PORT ="+ port.trim() +")))(CONNECT_DATA = (SERVICE_NAME ="+dbname+
                    ")(FAILOVER_MODE = (TYPE = SELECT)(METHOD = BASIC)(RETRIES = 180)(DELAY = 5))))";
            //
            // url = url2;
        } else if (DATABASETYPE.MYSQL.equals(dbtype)) {
            //
            url += "jdbc:mysql://";
            url += ip.trim();
            url += ":" + port.trim();
            url += "/" + dbname+POST_JDBC;
        } else if (DATABASETYPE.SQLSERVER.equals(dbtype)) {
            //
            url += "jdbc:jtds:sqlserver://";
            url += ip.trim();
            url += ":" + port.trim();
            url += "/" + dbname;
        } else if (DATABASETYPE.SQLSERVER2005.equals(dbtype)) {
            //
            url += "jdbc:sqlserver://";
            url += ip.trim();
            url += ":" + port.trim();
            url += "; DatabaseName=" + dbname;
        } else if (DATABASETYPE.DB2.equals(dbtype)) {
            url += "jdbc:db2://";
            url += ip.trim();
            url += ":" + port.trim();
            url += "/" + dbname;
        } else if (DATABASETYPE.INFORMIX.equals(dbtype)) {
            // Infox mix 可能有BUG
            url += "jdbc:informix-sqli://";
            url += ip.trim();
            url += ":" + port.trim();
            url += "/" + dbname;
            // +":INFORMIXSERVER=myserver;user="+bean.getDatabaseuser()+";password="+bean.getDatabasepassword()
        } else if (DATABASETYPE.SYBASE.equals(dbtype)) {
            url += "jdbc:sybase:Tds:";
            url += ip.trim();
            url += ":" + port.trim();
            url += "/" + dbname;
        }  else if (DATABASETYPE.HIVE.equals(dbtype)) {
            url += "jdbc:hive2://";
            url += ip.trim();
            url += ":" + port.trim();
            url += "/" + dbname;
            url += ";auth=noSasl" ;
        } else {
            throw new RuntimeException("不认识的数据库类型!");
        }
        //
        return url;
    }

    /**
     * 获取JDBC连接
     *
     * @param url
     * @param username
     * @param password
     * @return
     */
    public static Connection getConnection(String url, String username, String password) {
        Connection conn = null;
        try {
            // 不需要加载Driver. Servlet 2.4规范开始容器会自动载入
            // conn = DriverManager.getConnection(url, username, password);
            //
            Properties info =new Properties();
            //
            info.put("user", username);
            info.put("password", password);
            // !!! Oracle 如果想要获取元数据 REMARKS 信息,需要加此参数
            info.put("remarksReporting","true");
            // !!! MySQL 标志位, 获取TABLE元数据 REMARKS 信息
            info.put("useInformationSchema","true");
            // 不知道SQLServer需不需要设置...
            //
            conn = DriverManager.getConnection(url, info);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 将一个未处理的ResultSet解析为Map列表.
     *
     * @param rs
     * @return
     */
    public static List<Map<String, Object>> parseResultSetToMapList(ResultSet rs) {
        //
        List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
        //
        if (null == rs) {
            return null;
        }
        //
        try {
            while (rs.next()) {
                //
                Map<String, Object> map = parseResultSetToMap(rs);
                //
                if (null != map) {
                    result.add(map);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //
        return result;
    }

    /**
     * 解析ResultSet的单条记录,不进行 ResultSet 的next移动处理
     *
     * @param rs
     * @return
     */
    private static Map<String, Object> parseResultSetToMap(ResultSet rs) {
        //
        if (null == rs) {
            return null;
        }
        //
        Map<String, Object> map = new HashMap<String, Object>();
        //
        try {
            ResultSetMetaData meta = rs.getMetaData();
            //
            int colNum = meta.getColumnCount();
            //
            for (int i = 1; i <= colNum; i++) {
                // 列名
                String name = meta.getColumnLabel(i); // i+1
                Object value = rs.getObject(i);
                // 加入属性
                map.put(name, value);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        //
        return map;
    }

    //
    public static boolean TryLink(String databasetype, String ip, String port, String dbname, String username, String password) {
        //
        DATABASETYPE dbtype = parseDATABASETYPE(databasetype);
        String url = concatDBURL(dbtype, ip, port, dbname);
        Connection conn = null;
        //
        try {
            conn = getConnection(url, username, password);
            if(null == conn){
                return false;
            }
            DatabaseMetaData meta =  conn.getMetaData();
            //
            if(null == meta){
                return false;
            } else {
                // 只有这里返回true
                return true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally{
            close(conn);
        }
        //
        return false;
    }
    //
    public static void close(Connection conn) {
        if(conn!=null) {
            try {
                conn.close();
                conn = null;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    //
    public static void close(Statement stmt) {
        if(stmt!=null) {
            try {
                stmt.close();
                stmt = null;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    //
    public static void close(ResultSet rs) {
        if(rs!=null) {
            try {
                rs.close();
                rs = null;
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    //
    public static String trim(String str){
        if(null != str){
            str = str.trim();
        }
        return str;
    }

    public static void main(String[] args) {
        //testLinkOracle();
        //testLinkSQLServer();
    //    testOracle();
        //testMySQL();
        DATABASETYPE type=DATABASETYPE.MYSQL;
        System.out.println(getJDBCDrive(type));
    }

    //
    public static void testLinkOracle() {
        //
        String ip= "192.168.0.100";
        String port= "1521";
        String dbname= "orcl";
        String username= "username";
        String password= "*****pwd";
        String databasetype= "oracle";
        //
        String url = concatDBURL(parseDATABASETYPE(databasetype), ip, port, dbname);
        System.out.println("url="+url);
        //
        boolean result = TryLink(databasetype, ip, port, dbname, username, password);
        //
        System.out.println("result="+result);
    }


    //
    public static void testLinkSQLServer() {
        String ip= "dbtestyun.e6xayf.com";
        String port= "8089";
        String dbname= "E6PlateFormMain";
        String username= "sa";
        String password= "xa1101";
        String databasetype= "SQL Server2008";
        //
        String url = concatDBURL(parseDATABASETYPE(databasetype), ip, port, dbname);
        System.out.println("url="+url);
        //
        boolean result = TryLink(databasetype, ip, port, dbname, username, password);
        List<Map<String, Object>> tables = listTables(databasetype, ip, port, dbname, username, password);
        tables = MapUtil.convertKeyList2LowerCase(tables);
        System.out.println("result="+result);
        String jsonT = JSONArray.toJSONString(tables, true);
        System.out.println(jsonT);
        List<Map<String, Object>> columns = listColumns(databasetype, ip, port, dbname, username, password, "Base.E6Driver");
        //


        String jsonC = JSONArray.toJSONString(columns, true);
        System.out.println(jsonC);
        System.out.println("columns.size()=" + columns.size());
    }



    public static void testOracle() {
        //
        String ip = "192.168.8.213";
        String port = "1521";
        String dbname = "orcl";
        String username = "e3_user";
        String password = "!FE68jlj5rft55";
        //
        String databasetype = "Oracle";
        // DATABASETYPE dbtype = parseDATABASETYPE(databasetype);
        // System.out.println(DATABASETYPE.ORACLE.equals(dbtype));
        //
        String tableName = "E3_CRM.T_BSS_CORP";

        List<Map<String, Object>> tables = listTables(databasetype, ip, port, dbname, username, password);
        List<Map<String, Object>> columns = listColumns(databasetype, ip, port, dbname, username, password, tableName);
        //
        tables = MapUtil.convertKeyList2LowerCase(tables);
        columns = MapUtil.convertKeyList2LowerCase(columns);
        //
        String jsonT = JSONArray.toJSONString(tables, true);
        System.out.println(jsonT);
        System.out.println("tables.size()=" + tables.size());
        //
        System.out.println("-----------------------------------------" + "-----------------------------------------");
        System.out.println("-----------------------------------------" + "-----------------------------------------");
        //
        String jsonC = JSONArray.toJSONString(columns, true);
        System.out.println(jsonC);
        System.out.println("columns.size()=" + columns.size());
    }


    public static void testMySQL() {
        //
        String ip = "192.168.8.50";
        String port = "3306";
        String dbname = "cncounter";
        dbname = "car_warn";
        String username = "root";
        username = "car_warn";
        String password = "1q2w3e4r5t@";
        //
        String databasetype = "mysql";
        // DATABASETYPE dbtype = parseDATABASETYPE(databasetype);
        // System.out.println(DATABASETYPE.ORACLE.equals(dbtype));
        //
        String tableName = "tb_car_info";

        List<Map<String, Object>> tables = listTables(databasetype, ip, port, dbname, username, password);
        List<Map<String, Object>> columns = listColumns(databasetype, ip, port, dbname, username, password, tableName);
        //
        tables = MapUtil.convertKeyList2LowerCase(tables);
        columns = MapUtil.convertKeyList2LowerCase(columns);
        //
        String jsonT = JSONArray.toJSONString(tables, true);
        System.out.println(jsonT);
        System.out.println("tables.size()=" + tables.size());
        //
        System.out.println("-----------------------------------------" + "-----------------------------------------");
        System.out.println("-----------------------------------------" + "-----------------------------------------");
        //
        String jsonC = JSONArray.toJSONString(columns, true);
        System.out.println(jsonC);
        System.out.println("columns.size()=" + columns.size());
    }
    // 演示 DatabaseMetaData
    public static void demoDatabaseMetaData() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            Connection con = DriverManager.getConnection("jdbc:mysql://localhost/dbtest", "root", "root");
            //
            DatabaseMetaData dmd = con.getMetaData();
            System.out.println("当前数据库是：" + dmd.getDatabaseProductName());
            System.out.println("当前数据库版本：" + dmd.getDatabaseProductVersion());
            System.out.println("当前数据库驱动：" + dmd.getDriverVersion());
            System.out.println("当前数据库URL：" + dmd.getURL());
            System.out.println("当前数据库是否是只读模式？：" + dmd.isReadOnly());
            System.out.println("当前数据库是否支持批量更新？：" + dmd.supportsBatchUpdates());
            System.out.println("当前数据库是否支持结果集的双向移动（数据库数据变动不在ResultSet体现）？："
                    + dmd.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
            System.out.println("当前数据库是否支持结果集的双向移动（数据库数据变动会影响到ResultSet的内容）？："
                    + dmd.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE));
            System.out.println("========================================");

            ResultSet rs = dmd.getTables(null, null, "%", null);
            System.out.println("表名" + "," + "表类型");
            while (rs.next()) {
                System.out.println(rs.getString("TABLE_NAME") + "," + rs.getString("TABLE_TYPE"));
            }
            System.out.println("========================================");

            rs = dmd.getPrimaryKeys(null, null, "t_student");
            while (rs.next()) {
                System.out.println(rs.getString(3) + "表的主键是：" + rs.getString(4));
            }
            System.out.println("========================================");

            rs = dmd.getColumns(null, null, "t_student", "%");
            System.out.println("t_student表包含的字段:");
            while (rs.next()) {
                System.out.println(rs.getString(4) + " " + rs.getString(6) + "(" + rs.getString(7) + ");");
            }
            System.out.println("========================================");

        } catch (Exception e) {
            System.out.println("数据库操作出现异常");
        }
    }

    // ResultSetMetaData 使用示例
    // 此方法参考 http://blog.csdn.net/yirentianran/article/details/2950321
    public static void demoResultSetMetaData(ResultSetMetaData data) throws SQLException {
        for (int i = 1; i <= data.getColumnCount(); i++) {
            // 获得所有列的数目及实际列数
            int columnCount = data.getColumnCount();
            // 获得指定列的列名
            String columnName = data.getColumnName(i);
            // 获得指定列的列值
            // String columnValue = rs.getString(i);
            // 获得指定列的数据类型
            int columnType = data.getColumnType(i);
            // 获得指定列的数据类型名
            String columnTypeName = data.getColumnTypeName(i);
            // 所在的Catalog名字
            String catalogName = data.getCatalogName(i);
            // 对应数据类型的类
            String columnClassName = data.getColumnClassName(i);
            // 在数据库中类型的最大字符个数
            int columnDisplaySize = data.getColumnDisplaySize(i);
            // 默认的列的标题
            String columnLabel = data.getColumnLabel(i);
            // 获得列的模式
            String schemaName = data.getSchemaName(i);
            // 某列类型的精确度(类型的长度)
            int precision = data.getPrecision(i);
            // 小数点后的位数
            int scale = data.getScale(i);
            // 获取某列对应的表名
            String tableName = data.getTableName(i);
            // 是否自动递增
            boolean isAutoInctement = data.isAutoIncrement(i);
            // 在数据库中是否为货币型
            boolean isCurrency = data.isCurrency(i);
            // 是否为空
            int isNullable = data.isNullable(i);
            // 是否为只读
            boolean isReadOnly = data.isReadOnly(i);
            // 能否出现在where中
            boolean isSearchable = data.isSearchable(i);
            System.out.println(columnCount);
            System.out.println("获得列" + i + "的字段名称:" + columnName);
            // System.out.println("获得列" + i + "的字段值:" + columnValue);
            System.out.println("获得列" + i + "的类型,返回SqlType中的编号:" + columnType);
            System.out.println("获得列" + i + "的数据类型名:" + columnTypeName);
            System.out.println("获得列" + i + "所在的Catalog名字:" + catalogName);
            System.out.println("获得列" + i + "对应数据类型的类:" + columnClassName);
            System.out.println("获得列" + i + "在数据库中类型的最大字符个数:" + columnDisplaySize);
            System.out.println("获得列" + i + "的默认的列的标题:" + columnLabel);
            System.out.println("获得列" + i + "的模式:" + schemaName);
            System.out.println("获得列" + i + "类型的精确度(类型的长度):" + precision);
            System.out.println("获得列" + i + "小数点后的位数:" + scale);
            System.out.println("获得列" + i + "对应的表名:" + tableName);
            System.out.println("获得列" + i + "是否自动递增:" + isAutoInctement);
            System.out.println("获得列" + i + "在数据库中是否为货币型:" + isCurrency);
            System.out.println("获得列" + i + "是否为空:" + isNullable);
            System.out.println("获得列" + i + "是否为只读:" + isReadOnly);
            System.out.println("获得列" + i + "能否出现在where中:" + isSearchable);
        }
    }


}
