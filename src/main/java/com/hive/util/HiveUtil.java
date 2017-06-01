package com.hive.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by houlongbin on 2016/11/7.
 */
public class HiveUtil {
    private static final Log log = LogFactory.getLog(HiveUtil.class);
    private HiveUtil(){}
    private static class HiveJdbcUtilHolder {
        private static final HiveUtil hiveInstance = new HiveUtil();
    }
    public static final HiveUtil getInstance() {
        return HiveJdbcUtilHolder.hiveInstance;
    }
    static String HIVE_JDBC_IP = "";
    static String HIVE_USER = "";
    static String HIVE_PASS = "";
    static String HIVE_PORT="12000";
    static String HIVE_DATABASE="";
    static String HIVE_METASTORE_WAREHOUSE_DIR="";
    static {
        Properties properties = new Properties();

        try {
            properties.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("hive.properties"));
        } catch (Exception var2) {
            log.error("jdbc.properties must contains in classpath");
        }
        HIVE_JDBC_IP= properties.getProperty("hive.jdbc.url");
        log.info(" JDBC init HIVE_JDBC_IP=" + HIVE_JDBC_IP);
        HIVE_DATABASE = properties.getProperty("hive.jdbc.default.database");
        log.info(" JDBC init JDBC_URL=" + HIVE_DATABASE);
        HIVE_PORT =properties.getProperty("hive.jdbc.port") ;
        log.info(" JDBC init HIVE_PORT=" + HIVE_PORT);
        if(properties.getProperty("hive.jdbc.password")!=null){
            HIVE_PASS = properties.getProperty("hive.jdbc.password");
        }
        log.info(" JDBC init PASS=" + HIVE_PASS);
        HIVE_USER = properties.getProperty("hive.jdbc.user");
        log.info(" JDBC init PASS=" + HIVE_USER);
        HIVE_METASTORE_WAREHOUSE_DIR = properties.getProperty("hive.metastore.warehouse.dir");
        log.info(" JDBC init hive.metastore.warehouse.dir=" + HIVE_METASTORE_WAREHOUSE_DIR);
    }

    /**
     * hive 表是否存在
     * @param tableName
     * @return
     */
    public boolean validateTableExist(String tableName){
       return DBMSMetaUtil.validateTableExist("hive",HIVE_JDBC_IP,HIVE_PORT,HIVE_DATABASE,HIVE_USER,HIVE_PASS,tableName);
    }

    /**
     * 获取某一列最大值 为了增量导入
     * @param tableName
     * @param database
     * @param columns
     * @return
     */
    public String getMaxDefaultColumns(String tableName,String database,String columns)throws Exception{
        String resultStr="";
        String sql="SELECT MAX("+columns+") as cvalue FROM "+getHiveTableName(tableName,database);
        List<Map<String, Object>> result=DBMSMetaUtil.executeQuerySql("hive",HIVE_JDBC_IP,HIVE_PORT,HIVE_DATABASE,HIVE_USER,HIVE_PASS,sql);
        if(result!=null&&result.size()>0){
            resultStr=result.get(0).get("cvalue").toString();
        }
        log.info(" getMaxDefaultColumns tableName="+tableName+";database="+database+"columns="+columns+";sql="+sql+";result="+resultStr);
        return resultStr;
    }

    /**
     * 关系型数据库的表是否在hive中 如果不存在则建新表
     * @param tableName
     */
    public void initTable(String tableName,String datatype,String ip,String port,String database,String user,String pass){
        if(!validateTableExist(getHiveTableName(tableName,database))){
            createHiveTable(datatype,ip,port,database,user,pass,tableName);
        }
    }

    /**
     * 数据导入
     * @param hiveTable hiveTable
     * @param hdfsPath hdfs数据路径
     */
    public void loadData(String hiveTable,String hdfsPath)throws Exception{
        String sql = "load data  inpath '" + hdfsPath + "' into table "
                + hiveTable;
        DBMSMetaUtil.executeSql("hive",HIVE_JDBC_IP,HIVE_PORT,HIVE_DATABASE,HIVE_USER,HIVE_PASS,sql);
        log.info(" loadData hiveTable="+hiveTable+";hdfsPath="+hdfsPath);
    }
    /**
     * 创建hive表
     * @param datatype 数据库类型
     * @param ip 数据库ip
     * @param port 数据库端口
     * @param database 数据库名
     * @param user 数据库用户名
     * @param pass 数据库密码
     * @param tableName 数据库表
     * @return
     */
    public boolean createHiveTable(String datatype,String ip,String port,String database,String user,String pass,String tableName){
        boolean isSuccess=false;
        String createTableName=getHiveTableName(tableName,database);
        log.info(" createHiveTable table_name="+createTableName+";");
        StringBuilder stringBuilder=new StringBuilder(Constant.PRE_CREATE_HIVE_SQL).append(createTableName).append(Constant.PRE_CREATE_HIVE_BRACT);

        List<Map<String, Object>> columns = MapUtil.convertKeyList2LowerCase(DBMSMetaUtil.listColumns(datatype, ip, port, database, user, pass, tableName));
        log.info(" select meta table="+tableName+";columns="+JSONArray.toJSONString(columns, true));
        for(Map<String, Object> map:columns){
            stringBuilder.append(map.get("column_name").toString())
                        .append(" ")
                        .append(DBDaTaType.getHiveType(datatype,map.get("type_name").toString(),Integer.parseInt(map.get("column_size").toString()))
                        ) .append(" ");
            if(map.containsKey("remarks")){
                if(map.get("remarks")!=null){
                    stringBuilder.append( "comment '").append(map.get("remarks").toString()).append("'");
                }

            }
            stringBuilder.append(",");
        }
        stringBuilder.deleteCharAt(stringBuilder.length()-1);//去掉最后一个逗号
        stringBuilder.append(Constant.POST_CREATE_HIVE_SQL);
        System.out.println("datatype = [" + datatype + "], ip = [" + ip + "], port = [" + port + "], database = [" + database + "], user = [" + user + "], pass = [" + pass + "], tableName = [" + tableName + "]sql=["+stringBuilder.toString()+"]");
        log.info("hive sql="+stringBuilder.toString());
        DBMSMetaUtil.executeSql("hive",HIVE_JDBC_IP,HIVE_PORT,HIVE_DATABASE,HIVE_USER,HIVE_PASS,stringBuilder.toString());
        log.info("hive sql="+stringBuilder.toString());
        return isSuccess;
    }

    /**
     * 通过表和库拼凑hive表
     * @param tableName
     * @param database
     * @return
     */
    public static String getHiveTableName(String tableName,String database){
        String createTableName="";
        if(tableName.contains(".")){
            String[] schemaTable=tableName.split("\\.");
            String schemaPattern=schemaTable[0];
            String tableNamePattern=schemaTable[1];
            createTableName=Constant.SCHEMA+Constant.UNDERLINE+database+Constant.UNDERLINE+schemaPattern+Constant.UNDERLINE+Constant.TABLE+Constant.UNDERLINE+tableNamePattern;
        }else{
            createTableName=Constant.SCHEMA+Constant.UNDERLINE+database+Constant.UNDERLINE+Constant.TABLE+Constant.UNDERLINE+tableName;
        }
        return createTableName;
    }

    /**
     *根据hsql创建hive临时表
     * @param hsql hive sql
     * @param jdbcTableName 要导出的jdbc表名
     * @return
     */
    public String initTmpHiveTable(String hsql,String jdbcTableName){
        String tmpHiveTable=jdbcTableName+System.currentTimeMillis();
        StringBuffer hiveSql=new StringBuffer();
        hiveSql.append(Constant.PRE_CREATE_HIVE_SQL).append(tmpHiveTable).append(Constant.HIVE_BASE_FORMAT).append(Constant.SQL_AS).append(hsql);
        DBMSMetaUtil.executeSql("hive",HIVE_JDBC_IP,HIVE_PORT,HIVE_DATABASE,HIVE_USER,HIVE_PASS,hiveSql.toString());
        return tmpHiveTable;
    }

    /**
     * 获取hive表结构
     * @param hiveTable
     * @return
     */
    public  List<Map<String, Object>>  getHiveListColumns(String hiveTable){
        List<Map<String, Object>> result=MapUtil.convertKeyList2LowerCase(DBMSMetaUtil.listColumns("hive",HIVE_JDBC_IP,HIVE_PORT,HIVE_DATABASE,HIVE_USER,HIVE_PASS, hiveTable));
        log.info(" getHiveListColumns hiveTable = [" + hiveTable + "];result="+JSON.toJSONString(result));
        return result;
    }

    /**
     * 根据hive库名及表名 查找hdfs路径
     *  /opt/hive-1.1.0-cdh5.4.4/warehouse/test.db/e6corp2/
     * @param hiveTable
     * @param db
     * @return
     */
    public String getHiveHdfsPath(String hiveTable,String db){
        if(StringUtils.isNotBlank(db)){
            return HIVE_METASTORE_WAREHOUSE_DIR+"/"+db+Constant.POINT_DB+ "/"+hiveTable+"/";
        }else{
            return HIVE_METASTORE_WAREHOUSE_DIR+"/"+hiveTable+"/";
        }
    }

    /**
     * 删除hive表
     * @param hiveTable
     * @param db
     */
    public void dropHiveTable(String hiveTable,String db){
        //db为空默认删除default
        if(StringUtils.isNotBlank(db)){
            hiveTable=db+Constant.POINT+hiveTable;
        }
        String sql="DROP TABLE "+hiveTable;
        DBMSMetaUtil.executeSql("hive",HIVE_JDBC_IP,HIVE_PORT,HIVE_DATABASE,HIVE_USER,HIVE_PASS,sql);
    }
    public static void main(String[] args) {
//        System.out.println(DBDaTaType.getHiveType("sqlserver 2008","varchar",20));
//        String tableName="Base.E6Driver";
//        String createTableName="";
//        if(tableName.contains(".")){
//            String[] schemaTable=tableName.split("\\.");
//            String schemaPattern=schemaTable[0];
//            String tableNamePattern=schemaTable[1];
//            createTableName=Constant.SCHEMA+Constant.UNDERLINE+"E6PlateFormMain"+Constant.UNDERLINE+schemaPattern+Constant.UNDERLINE+Constant.TABLE+Constant.UNDERLINE+tableNamePattern;
//        }else{
//            createTableName=Constant.SCHEMA+Constant.UNDERLINE+"E6PlateFormMain"+Constant.UNDERLINE+Constant.TABLE+Constant.UNDERLINE+tableName;
//        }
//        System.out.println(createTableName);
        String ip= "192.168.8.207";
        String port= "10001";
        String dbname= "default";
        String username= "hadoop";
        String password= "";
        String databasetype= "hive";
        List<Map<String, Object>> result=MapUtil.convertKeyList2LowerCase(DBMSMetaUtil.listColumns(databasetype, ip, port, dbname, username, "", "schema_orcl_e3_crm_table_t_bss_corp"));
        System.out.println("josn="+JSON.toJSONString(result));
    }

}
