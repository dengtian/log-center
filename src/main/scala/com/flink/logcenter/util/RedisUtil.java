package com.flink.logcenter.util;

import cn.hutool.core.util.StrUtil;
import cn.hutool.setting.Setting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Collections;
import java.util.Map;

public class RedisUtil {
    private static Logger logger = LoggerFactory.getLogger(RedisUtil.class);

    private static JedisPool jedisPool = null;

    private static final int DB = 2;

    private static Jedis getRedis() {
        if (jedisPool == null) {
            JedisPoolConfig config = new JedisPoolConfig();
            Setting setting = new Setting("redis.setting");

            config.setMaxTotal(setting.getInt("maxTotal"));
            config.setMaxIdle(setting.getInt("maxIdle"));
            config.setMaxWaitMillis(setting.getInt("maxWaitMillis"));

            config.setTestOnBorrow(true);
            jedisPool = new JedisPool(config, setting.getStr("host"), setting.getInt("port"), 3000);
        }
        Jedis resource = jedisPool.getResource();
        if (resource != null) {
            resource.select(DB);
        }
        return resource;
    }


    public static void hIncrBy(String key, String field, long value) {
        Jedis redis = null;
        try {
            redis = getRedis();
            redis.hincrBy(key, field, value);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (redis != null) {
                redis.close();
            }
        }
    }

    public static Map<String, String> hGetAll(String key) {
        Jedis redis = null;
        try {
            redis = getRedis();
            return redis.hgetAll(key);
        } catch (Exception e) {
            e.printStackTrace();
            return Collections.emptyMap();
        } finally {
            if (redis != null) {
                redis.close();
            }
        }
    }

    public static String hGet(String key,String field) {
        Jedis redis = null;
        try {
            redis = getRedis();
            return redis.hget(key,field);
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        } finally {
            if (redis != null) {
                redis.close();
            }
        }
    }

    public static long ttl(String key) {
        Jedis redis = null;
        try {
            redis = getRedis();
            return redis.ttl(key);
        } catch (Exception e) {
            e.printStackTrace();
            return -3;
        } finally {
            if (redis != null) {
                redis.close();
            }
        }
    }

    public static void setAndExpire(String key, long value, int secs) {
        if (StrUtil.isNotEmpty(key)) {
            Jedis redis = null;
            try {
                redis = getRedis();
                redis.set(key, String.valueOf(value));
                redis.expire(key, secs);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (redis != null) {
                    redis.close();
                }
            }
        }
    }

    public static void hSetAndExpire(String key, String field, long value, int secs) {
        if (StrUtil.isNotEmpty(key)) {
            Jedis redis = null;
            try {
                redis = getRedis();
                redis.hset(key, field, String.valueOf(value));
                redis.expire(key, secs);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (redis != null) {
                    redis.close();
                }
            }
        }
    }

    public static void increaseExpireAt(String key, long value, long timestamp) {
        if (StrUtil.isNotEmpty(key)) {
            Jedis redis = null;
            try {
                redis = getRedis();
                redis.incrBy(key, value);
                redis.expireAt(key, timestamp);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (redis != null) {
                    redis.close();
                }
            }
        }
    }

    public static void hIncreaseExpireAt(String key, String field, long value, long timestamp) {
        if (StrUtil.isNotEmpty(key)) {
            Jedis redis = null;
            try {
                redis = getRedis();
                redis.hincrBy(key, field, value);
                redis.expireAt(key, timestamp);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                if (redis != null) {
                    redis.close();
                }
            }
        }
    }



}
