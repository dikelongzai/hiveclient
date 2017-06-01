package com.test;

import com.hive.util.DBMSMetaUtil;
import com.hive.util.HiveUtil;
import junit.framework.TestCase;

/**
 * Created by houlongbin on 2016/11/7.
 */
public class DBMSMetaUtilTest extends TestCase {
    public void testConnect(){
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
//        DBMSMetaUtil.testMySQL();
//        DBMSMetaUtil.testLinkSQLServer();
        DBMSMetaUtil.testOracle();
//        HiveUtil.getInstance().createHiveTable(databasetype,ip,port,dbname,username,password,tableName);
    }
    public  void testLinkhive() {
//        String ip= "192.168.8.207";
//        String port= "10001";
//        String dbname= "default";
//        String username= "hadoop";
//        String password= "";
//        String databasetype= "hive";
//        String url = DBMSMetaUtil.concatDBURL(DBMSMetaUtil.parseDATABASETYPE(databasetype), ip, port, dbname);
//        System.out.println("url="+url);
//        //
//        boolean result = DBMSMetaUtil.TryLink(databasetype, ip, port, dbname, username, password);
//        //
//        System.out.println("result="+result);
    }
}
