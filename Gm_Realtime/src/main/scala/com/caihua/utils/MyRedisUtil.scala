package com.caihua.utils

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object MyRedisUtil {
  //1.创建Jedis连接池变量
  private var jedisPool: JedisPool = null

  def getJedis():Jedis ={
    //1.判断连接池是否为空
    if(jedisPool == null){
      println("开辟一个连接池")
      //① 从配置文件获取配置信息
      val properties: Properties = MyPropertiesUtil.load("config.properties")
      val redis_host: String = properties.getProperty("redis.host")
      val redis_port: String = properties.getProperty("redis.port")

      //② 设置连接池参数
      val poolConfig = new JedisPoolConfig()
      poolConfig.setMaxTotal(100) //最大连接数
      poolConfig.setMaxIdle(20) //最大空闲
      poolConfig.setMinIdle(20) //最小空闲
      poolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      poolConfig.setMaxWaitMillis(500) //等待时长
      poolConfig.setTestOnBorrow(true) //每次获得连接都进行测试

      //③ 创建连接池
      jedisPool = new JedisPool(poolConfig,redis_host,redis_port.toInt)
    }
//    println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
//    println("获取一个连接")

    //2.返回Jedis连接对象
    jedisPool.getResource
  }
}
