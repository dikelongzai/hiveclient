package com.hive.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by houlongbin on 2016/11/28.
 */
public class CommonJDBCUtil {
    private static final Log log = LogFactory.getLog(CommonJDBCUtil.class);

    /**
     * 根据hive表建jdbc表
     *
     * @param listColumns
     * @param databasetype
     * @param jdbcTable
     * @return
     */
    public static String getCreateSqlByHiveTmpTable(List<Map<String, Object>> listColumns, String databasetype, String jdbcTable) {
        StringBuilder stringBuilder = new StringBuilder(Constant.PRE_CREATE_HIVE_SQL).append(jdbcTable).append(Constant.PRE_CREATE_HIVE_BRACT);
        log.info(" select meta columns=" + JSONArray.toJSONString(listColumns, true));
        for (Map<String, Object> map : listColumns) {
            stringBuilder.append(map.get("column_name").toString()).append(" ");
            for(Map.Entry<String, Object> tmp:map.entrySet()){
                log.info(" map ente key="+tmp.getKey()+";value="+tmp.getValue());
            }
            if (map.containsKey("decimal_digits")) {
                    if(map.get("decimal_digits")!=null){
                        stringBuilder.append(DBDaTaType.getJDBCTypeByHive(databasetype, map.get("type_name").toString(), Integer.parseInt(map.get("column_size").toString()), Integer.parseInt(map.get("decimal_digits").toString())));
                    }else{
                        stringBuilder.append(DBDaTaType.getJDBCTypeByHive(databasetype, map.get("type_name").toString(), Integer.parseInt(map.get("column_size").toString()), 0));
                    }


            } else {
                stringBuilder.append(DBDaTaType.getJDBCTypeByHive(databasetype, map.get("type_name").toString(), Integer.parseInt(map.get("column_size").toString()), 0));
            }

            stringBuilder.append(" ");
            stringBuilder.append(",");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);//去掉最后一个逗号
        stringBuilder.append(Constant.PRE_RIGHT_CREATE_HIVE_BRACT);
        log.info("listColumns = [" + listColumns + "], databasetype = [" + databasetype + "], jdbcTable = [" + jdbcTable + "],getCreateSqlByHiveTmpTable sql=["+stringBuilder.toString()+"]");
        return stringBuilder.toString();

    }

    /**
     * 根据hive table创建jdbctable
     *
     * @param datatype
     * @param ip
     * @param port
     * @param database
     * @param user
     * @param pass
     * @param tableName
     * @param hiveTable
     * @return
     * @throws Exception
     */
    public static boolean createJdbcTable(String datatype, String ip, String port, String database, String user, String pass, String tableName, String hiveTable) throws Exception {
        boolean isSuccess = false;
        try {
            if (!DBMSMetaUtil.validateTableExist(datatype, ip, port, database, user, pass, tableName)) {
                List<Map<String, Object>> listColumns = HiveUtil.getInstance().getHiveListColumns(hiveTable);
                String sql = getCreateSqlByHiveTmpTable(listColumns, datatype, tableName);
                DBMSMetaUtil.executeSql(datatype, ip, port, database, user, pass, sql);
                log.info(" success createJdbcTable datatype = [" + datatype + "], ip = [" + ip + "], port = [" + port + "], database = [" + database + "], user = [" + user + "], pass = [" + pass + "], tableName = [" + tableName + "], hiveTable = [" + hiveTable + "]");
            }
            isSuccess = true;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return isSuccess;

    }

    public static void main(String[] args) {
        String jsonArr = "[{\"column_name\":\"createduserid\",\"column_size\":10,\"data_type\":4,\"decimal_digits\":0,\"is_auto_increment\":\"NO\",\"is_nullable\":\"YES\",\"nullable\":1,\"num_prec_radix\":10,\"ordinal_position\":1,\"table_name\":\"hive2jdbc1480435860064\",\"table_schem\":\"default\",\"type_name\":\"INT\"},{\"column_name\":\"createdtime\",\"column_size\":2147483647,\"data_type\":12,\"is_auto_increment\":\"NO\",\"is_nullable\":\"YES\",\"nullable\":1,\"ordinal_position\":2,\"table_name\":\"hive2jdbc1480435860064\",\"table_schem\":\"default\",\"type_name\":\"STRING\"},{\"column_name\":\"corpname\",\"column_size\":64,\"data_type\":12,\"is_auto_increment\":\"NO\",\"is_nullable\":\"YES\",\"nullable\":1,\"ordinal_position\":3,\"table_name\":\"hive2jdbc1480435860064\",\"table_schem\":\"default\",\"type_name\":\"VARCHAR\"},{\"column_name\":\"_c3\",\"column_size\":10,\"data_type\":4,\"decimal_digits\":0,\"is_auto_increment\":\"NO\",\"is_nullable\":\"YES\",\"nullable\":1,\"num_prec_radix\":10,\"ordinal_position\":4,\"table_name\":\"hive2jdbc1480435860064\",\"table_schem\":\"default\",\"type_name\":\"INT\"}]";
        JSONArray jsonArray = JSON.parseArray(jsonArr);
        List<Map<String, Object>> listColumns = new LinkedList<Map<String, Object>>();
        Iterator<Object> iterator = jsonArray.iterator();
        while (iterator.hasNext()) {
            JSONObject json = (JSONObject) iterator.next();
            listColumns.add(JSON.parseObject(json.toJSONString(), Map.class));
        }
        System.out.println(getCreateSqlByHiveTmpTable(listColumns, "mysql", "testTable"));
        log.info(getCreateSqlByHiveTmpTable(listColumns, "mysql", "testTable"));;

    }

}
