package com.hive.util;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by houlongbin on 2016/11/7.
 */
public class DBDaTaType {

    private static final Logger log = Logger.getLogger(DBDaTaType.class);
    public static final String DATABASE_TYPE_HIVE = "hive";
    public static final String DATABASE_TYPE_MSSQL = "SQL Server2008";
    public static final String DATABASE_TYPE_MYSQL = "mysql";
    public static final String DATABASE_TYPE_ORACLE = "oracle";


    public static final String DATA_TYPE_BOOLEAN = "BOOLEAN";
    public static final String DATA_TYPE_TINYINT = "TINYINT";
    public static final String DATA_TYPE_SMALLINT = "SMALLINT";
    public static final String DATA_TYPE_INT = "INT";
    public static final String DATA_TYPE_BIGINT = "BIGINT";
    public static final String DATA_TYPE_FLOAT = "FLOAT";
    public static final String DATA_TYPE_DOUBLE = "DOUBLE";
    public static final String DATA_TYPE_DEICIMAL = "DEICIMAL";
    public static final String DATA_TYPE_STRING = "STRING";
    public static final String DATA_TYPE_VARCHAR = "VARCHAR";
    public static final String DATA_TYPE_VARCHAR2 = "VARCHAR2";
    public static final String DATA_TYPE_NVARCHAR2 = "NVARCHAR2";
    public static final String DATA_TYPE_CHAR = "CHAR";
    public static final String DATA_TYPE_BINARY = "BINARY";
    public static final String DATA_TYPE_TIMESTAMP = "TIMESTAMP";
    public static final String DATA_TYPE_DATE = "DATE";
    public static final String DATA_TYPE_NUMBER = "NUMBER";
    public static final String DATA_TYPE_NUMBERIC = "NUMBERIC";
    public static final String DATA_TYPE_MONEY = "MONEY";
    public static final String DATA_TYPE_REAL = "REAL";
    public static final String DATA_TYPE_DATETIME = "DATETIME";
    public static final String DATA_TYPE_SMALLDATETIME = "SMALLDATETIME";
    public static final String DATA_TYPE_NVARCHAR = "NVARCHAR";
    public static final String DATA_TYPE_TEXT = "TEXT";
    public static final String DATA_TYPE_NTEXT = "NTEXT";
    public static final String DATA_TYPE_NCHAR = "NCHAR";

    static List<String> ALL_COLUMNS_TYPE = Arrays.asList(
            DATA_TYPE_BOOLEAN, DATA_TYPE_TINYINT, DATA_TYPE_SMALLINT,
            DATA_TYPE_INT, DATA_TYPE_BIGINT, DATA_TYPE_FLOAT,
            DATA_TYPE_DOUBLE, DATA_TYPE_DEICIMAL, DATA_TYPE_STRING,
            DATA_TYPE_VARCHAR, DATA_TYPE_CHAR, DATA_TYPE_BINARY,
            DATA_TYPE_TIMESTAMP, DATA_TYPE_DATE, DATA_TYPE_NUMBERIC,
            DATA_TYPE_MONEY, DATA_TYPE_REAL, DATA_TYPE_DATETIME,
            DATA_TYPE_SMALLDATETIME, DATA_TYPE_NVARCHAR,
            DATA_TYPE_TEXT, DATA_TYPE_NTEXT, DATA_TYPE_NCHAR, DATA_TYPE_NUMBER
    );
    static Map<String, String> MAP_MSSQL_HIME_TYPE = new HashMap<String, String>() {
        {
            put(DATA_TYPE_DATETIME, DATA_TYPE_STRING);
            put(DATA_TYPE_MONEY, DATA_TYPE_DEICIMAL + "(9,2)");
            put(DATA_TYPE_TINYINT, DATA_TYPE_INT);
            put(DATA_TYPE_NVARCHAR, DATA_TYPE_VARCHAR);
        }
    };
    static Map<String, String> MAP_MYSQL_HIVE_TYPE = new HashMap<String, String>() {
        {
            put(DATA_TYPE_DATETIME, DATA_TYPE_STRING);
            put(DATA_TYPE_MONEY, DATA_TYPE_DEICIMAL + "(9,2)");
            put(DATA_TYPE_TINYINT, DATA_TYPE_INT);
            put(DATA_TYPE_NVARCHAR, DATA_TYPE_VARCHAR);
            put(DATA_TYPE_TEXT, DATA_TYPE_STRING);
        }
    };
    static Map<String, String> MAP_ORACLE_HIVE_TYPE = new HashMap<String, String>() {
        {
            put(DATA_TYPE_TINYINT, DATA_TYPE_INT);
            put(DATA_TYPE_NVARCHAR, DATA_TYPE_VARCHAR);
            put(DATA_TYPE_VARCHAR2, DATA_TYPE_VARCHAR);
            put(DATA_TYPE_DATE, DATA_TYPE_STRING);
            put(DATA_TYPE_NVARCHAR2, DATA_TYPE_STRING);
            put(DATA_TYPE_NUMBER, DATA_TYPE_FLOAT);

        }

        ;

    };
    static Map<String, String> MAP_HIVE_MYSQL_TYPE = new HashMap<String, String>() {
        {
            put(DATA_TYPE_STRING, DATA_TYPE_VARCHAR);

        }


    };
    static Map<String, String> MAP_HIVE_MSSQL_TYPE = new HashMap<String, String>() {
        {
            put(DATA_TYPE_STRING, DATA_TYPE_VARCHAR);
//            put(DATA_TYPE_NVARCHAR, DATA_TYPE_VARCHAR);
//            put(DATA_TYPE_VARCHAR2, DATA_TYPE_VARCHAR);
//            put(DATA_TYPE_DATE, DATA_TYPE_STRING);
//            put(DATA_TYPE_NVARCHAR2, DATA_TYPE_STRING);
//            put(DATA_TYPE_NUMBER, DATA_TYPE_FLOAT);

        }


    };

    public static String getHiveType(String dbType, String columnsType, int COLUMN_SIZE) {
        String hiveType = "";
        if (DBMSMetaUtil.parseDATABASETYPE(dbType).equals(DBMSMetaUtil.DATABASETYPE.SQLSERVER2005)) {
            if (MAP_MSSQL_HIME_TYPE.containsKey(columnsType.trim().toUpperCase())) {
                hiveType = MAP_MSSQL_HIME_TYPE.get(columnsType.trim().toUpperCase()).toLowerCase();

            } else {
                if (ALL_COLUMNS_TYPE.contains(columnsType.trim().toUpperCase())) {
                    hiveType = columnsType.toLowerCase();
                } else {
                    log.info("unkonw column type columnsType=" + columnsType + ";return default=" + DATA_TYPE_STRING);
                    hiveType = DATA_TYPE_STRING.toLowerCase();

                }

            }
        } else if (DBMSMetaUtil.parseDATABASETYPE(dbType).equals(DBMSMetaUtil.DATABASETYPE.MYSQL)) {
            if (MAP_MYSQL_HIVE_TYPE.containsKey(columnsType.trim().toUpperCase())) {
                hiveType = MAP_MYSQL_HIVE_TYPE.get(columnsType.trim().toUpperCase()).toLowerCase();

            } else {
                if (ALL_COLUMNS_TYPE.contains(columnsType.trim().toUpperCase())) {
                    hiveType = columnsType.toLowerCase();
                } else {
                    log.info("unkonw column type columnsType=" + columnsType + ";return default=" + DATA_TYPE_STRING);
                    hiveType = DATA_TYPE_STRING.toLowerCase();

                }

            }
        } else if (DBMSMetaUtil.parseDATABASETYPE(dbType).equals(DBMSMetaUtil.DATABASETYPE.ORACLE)) {
            if (MAP_ORACLE_HIVE_TYPE.containsKey(columnsType.trim().toUpperCase())) {
                hiveType = MAP_ORACLE_HIVE_TYPE.get(columnsType.trim().toUpperCase()).toLowerCase();

            } else {
                if (ALL_COLUMNS_TYPE.contains(columnsType.trim().toUpperCase())) {
                    hiveType = columnsType.toLowerCase();
                } else {
                    log.info("unkonw column type columnsType=" + columnsType + ";return default=" + DATA_TYPE_STRING);
                    hiveType = DATA_TYPE_STRING.toLowerCase();

                }

            }
        }
        if (StringUtils.isNotEmpty(hiveType) && hiveType.contains(DATA_TYPE_CHAR.toLowerCase())) {
            hiveType = hiveType + Constant.PRE_CREATE_HIVE_BRACT + COLUMN_SIZE + Constant.PRE_RIGHT_CREATE_HIVE_BRACT;
        }
        return hiveType;

    }

    public static String getJDBCTypeByHive(String dbType, String hiveColumnsType, int COLUMN_SIZE, int DECIMAL_DIGITS) {
        String JDBCType = "";
        if(COLUMN_SIZE>Constant.MAX_VARCHAR_LENGTH){
            COLUMN_SIZE=Constant.MAX_VARCHAR_LENGTH;
        }
        if (DBMSMetaUtil.parseDATABASETYPE(dbType).equals(DBMSMetaUtil.DATABASETYPE.MYSQL)) {
            if (MAP_HIVE_MYSQL_TYPE.containsKey(hiveColumnsType.trim().toUpperCase())) {
                JDBCType = MAP_HIVE_MYSQL_TYPE.get(hiveColumnsType.trim().toUpperCase()).toLowerCase();
            } else {
                if (ALL_COLUMNS_TYPE.contains(hiveColumnsType.trim().toUpperCase())) {
                    JDBCType = hiveColumnsType.toLowerCase();
                } else {
                    log.info("unkonw column type columnsType=" + hiveColumnsType + ";return default=" + DATA_TYPE_STRING);
                    JDBCType = DATA_TYPE_TEXT.toLowerCase();

                }

            }
        }else if(DBMSMetaUtil.parseDATABASETYPE(dbType).equals(DBMSMetaUtil.DATABASETYPE.SQLSERVER2005)){
            if (MAP_HIVE_MSSQL_TYPE.containsKey(hiveColumnsType.trim().toUpperCase())) {
                JDBCType = MAP_HIVE_MSSQL_TYPE.get(hiveColumnsType.trim().toUpperCase()).toLowerCase();
            } else {
                if (ALL_COLUMNS_TYPE.contains(hiveColumnsType.trim().toUpperCase())) {
                    JDBCType = hiveColumnsType.toLowerCase();
                } else {
                    log.info("unkonw column type columnsType=" + hiveColumnsType + ";return default=" + DATA_TYPE_STRING);
                    JDBCType = DATA_TYPE_TEXT.toLowerCase();

                }

            }
        }
        if (StringUtils.isNotEmpty(JDBCType) && (JDBCType.contains(DATA_TYPE_CHAR.toLowerCase()) || JDBCType.contains(DATA_TYPE_INT.toLowerCase()))) {
            if(!DBMSMetaUtil.parseDATABASETYPE(dbType).equals(DBMSMetaUtil.DATABASETYPE.SQLSERVER2005)){
                //mssql int 不加位数
                JDBCType = JDBCType + Constant.PRE_CREATE_HIVE_BRACT + COLUMN_SIZE + Constant.PRE_RIGHT_CREATE_HIVE_BRACT;
            }

        }
        if (JDBCType.contains(DATA_TYPE_FLOAT.toLowerCase())
                || JDBCType.contains(DATA_TYPE_DOUBLE.toLowerCase())
                || JDBCType.contains(DATA_TYPE_DOUBLE.toLowerCase())
                ||JDBCType.contains(DATA_TYPE_NUMBER.toLowerCase())
                ||JDBCType.contains(DATA_TYPE_DEICIMAL.toLowerCase())
                ) {

            JDBCType = JDBCType + Constant.PRE_CREATE_HIVE_BRACT +COLUMN_SIZE +Constant.COMMA+DECIMAL_DIGITS+ Constant.PRE_RIGHT_CREATE_HIVE_BRACT;

        }
        return JDBCType;

    }


}
