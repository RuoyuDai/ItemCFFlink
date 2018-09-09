package com;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class JedisConnectionPool {
    private static JedisPool pool;
    static{
        JedisPoolConfig config=new JedisPoolConfig();
        config.setMaxTotal(1000);
        config.setMaxIdle(10);
        pool = new JedisPool(config, "127.0.0.1", 6379);
    }
    public static Jedis getJedis(){
        return pool.getResource();
    }

}
