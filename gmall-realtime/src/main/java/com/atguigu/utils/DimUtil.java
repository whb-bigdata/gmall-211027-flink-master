package com.atguigu.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.common.GmallConfig;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public class DimUtil {

    public static JSONObject getDimInfo(Connection connection, String tableName, String id) throws Exception {

        //1.查询Redis
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        String dimInfoStr = jedis.get(redisKey);
        if (dimInfoStr != null) {
            //重置数据的过期时间
            jedis.expire(redisKey, 24 * 60 * 60);
            //归还连接
            jedis.close();
            //返回结果
            return JSON.parseObject(dimInfoStr);
        }

        //拼接SQL
        String querySql = "select * from " + GmallConfig.HBASE_SCHEMA + "." + tableName + " where id ='" + id + "'";
        System.out.println("查询SQL为：" + querySql);

        //查询Phoenix
        List<JSONObject> list = JdbcUtil.queryList(connection, querySql, JSONObject.class, false);
        JSONObject dimInfo = list.get(0);

        //将数据写入Redis
        jedis.set(redisKey, dimInfo.toJSONString());
        jedis.expire(redisKey, 24 * 60 * 60);
        jedis.close();

        //返回结果数据
        return dimInfo;
    }

    //删除维表数据
    public static void delDimInfo(String tableName, String id) {
        Jedis jedis = JedisUtil.getJedis();
        String redisKey = "DIM:" + tableName + ":" + id;
        jedis.del(redisKey);
        jedis.close();
    }

    public static void main(String[] args) throws Exception {

        Connection connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);

        long start = System.currentTimeMillis();
        JSONObject dimInfo = getDimInfo(connection, "DIM_BASE_CATEGORY1", "19");
        long end = System.currentTimeMillis();
        JSONObject dimInfo2 = getDimInfo(connection, "DIM_BASE_CATEGORY1", "19");
        long end2 = System.currentTimeMillis();
        JSONObject dimInfo3 = getDimInfo(connection, "DIM_BASE_CATEGORY1", "19");
        long end3 = System.currentTimeMillis();
        JSONObject dimInfo4 = getDimInfo(connection, "DIM_BASE_CATEGORY1", "19");
        long end4 = System.currentTimeMillis();

        System.out.println(end - start); //145  138  148  136   140
        System.out.println(end2 - end);  //6    8    15   15    10
        System.out.println(end3 - end2); //1    0    1    1
        System.out.println(end4 - end3);

        System.out.println(dimInfo);
        System.out.println(dimInfo2);

        connection.close();
    }

}
