package com.jesson.session.topmerchant;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @Auther: jesson
 * @Date: 2018/9/6 17:26
 * @Description:
 */
public class JedisUtil {
    private static JedisPool pool = null;

    /**
     * 获取jedis连接池
     * */
    public static JedisPool getPool(){
        if(pool == null){
            //创建jedis连接池配置
            JedisPoolConfig config = new JedisPoolConfig();
            //最大连接数
            config.setMaxTotal(20);
            //最大空闲连接
            config.setMaxIdle(5);
            //创建redis连接池
            // 使用jedisPool的时候，timeout一定要给出来，如果不给，redis很大概率会报错，超时
            pool = new JedisPool(config,"bigdata01.com",6379,3000);
        }
        return pool;
    }

    /**
     * 获取jedis连接
     * */
    public static Jedis getConn(){
        Jedis jedis= getPool().getResource();
        jedis.auth("jesson");
        return jedis;
    }

    /**
     * 测试连接
     * @param args
     */
    public static void main(String[] args) {
        Jedis jedis = getPool().getResource();
        jedis.incrBy("mine", 5);
        jedis.close();
    }
}
