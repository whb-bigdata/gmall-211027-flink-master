package com.atguigu.app.func;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DimUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

public class DimSinkFunction extends RichSinkFunction<JSONObject> {

    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
    }

    //value：{"sinkTable":"dim_xxx","database":"gmall","table":"base_trademark","type":"insert","ts":1592270938,"xid":13090,"xoffset":1573,"data":{"id":"12","tm_name":"atguigu"},"old":{}}
    @Override
    public void invoke(JSONObject value, Context context) throws Exception {

        PreparedStatement preparedStatement = null;

        try {
            //拼接SQL  upsert into db.tn(id,tm_name) values ('12','atguigu')
            String sinkTable = value.getString("sinkTable");
            JSONObject data = value.getJSONObject("data");
            String upsertSql = genUpsertSql(sinkTable, data);
            System.out.println(upsertSql);

            //预编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);

            //如果当前为更新数据,则需要删除缓存数据
            if ("update".equals(value.getString("type"))) {
                DimUtil.delDimInfo(sinkTable.toUpperCase(), data.getString("id"));
            }

            //执行写入操作
            preparedStatement.execute();
            connection.commit();

        } catch (SQLException e) {
            System.out.println("插入数据失败！");
        } finally {
            //释放资源
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    /**
     * @param sinkTable tn
     * @param data      {"id":"12","tm_name":"atguigu"}
     * @return upsert into db.tn(id,tm_name,logo_url) values ('12','atguigu','/aaa/bbb')
     */
    private String genUpsertSql(String sinkTable, JSONObject data) {

        Set<String> columns = data.keySet();
        Collection<Object> values = data.values();

        //scala:list.mkString(",")   ["1","2","3"] => "1,2,3"
        return "upsert into " + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "(" +
                StringUtils.join(columns, ",") + ") values ('" +
                StringUtils.join(values, "','") + "')";
    }
}
