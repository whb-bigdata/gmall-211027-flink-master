package com.atguigu.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisUtil {

    private static JedisPool jedisPool;

    private static void initJedisPool() {
        System.out.println("----初始化Redis连接池-----");
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(100);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(5);
        poolConfig.setBlockWhenExhausted(true);
        poolConfig.setMaxWaitMillis(2000);
        poolConfig.setTestOnBorrow(true);
        jedisPool = new JedisPool(poolConfig, "hadoop104", 6379, 10000);
    }

    public static Jedis getJedis() {
        if (jedisPool == null) {
            initJedisPool();
        }
//        System.out.println("----获取Jedis客户端-----");
        return jedisPool.getResource();
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        String pong = jedis.ping();
        System.out.println(pong);
    }

}