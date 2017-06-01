package com.hive.util;

/**
 * Created by houlongbin on 2016/11/10.
 */
public class Constant {
    static final String PRE_CREATE_HIVE_SQL="create table ";
    static final String PRE_CREATE_HIVE_BRACT="(";
    static final String PRE_RIGHT_CREATE_HIVE_BRACT=")";
    static final String HIVE_BASE_FORMAT=" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE ";
    static final String POST_CREATE_HIVE_SQL=PRE_RIGHT_CREATE_HIVE_BRACT+HIVE_BASE_FORMAT;
    static  final String SCHEMA="schema";
    static  final String TABLE="table";
    static  final String SQL_AS=" as ";
    static  final String UNDERLINE="_";
    static  final String COMMA=",";
    static  final String POINT_DB=".db";
    static  final String POINT=".";
    static final int  MAX_VARCHAR_LENGTH=1024;
    public static final String POST_JDBC="?generateSimpleParameterMetadata=true&&characterEncoding=UTF-8";
    //static final String  HIVE_TMP_TABLE_SQL=POST_CREATE_HIVE_SQL.replaceAll(")","");


}
