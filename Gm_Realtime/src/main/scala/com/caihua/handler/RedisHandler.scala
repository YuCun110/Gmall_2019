package com.caihua.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.caihua.bean.StartUpLog
import com.caihua.utils.MyRedisUtil
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object RedisHandler {

  //1.创建时间格式化对象
  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  /**
   * 将当天第一次登录的用户信息，存入Redis中
   * @param filterByHistoryDStream 当天第一登录的用户数据集
   */
  def saveToRedis(filterByHistoryDStream: DStream[StartUpLog]) = {
    filterByHistoryDStream.foreachRDD(rdd => {
      //1.遍历每个分区的数据
      rdd.foreachPartition(iter => {
        iter.foreach(starUpLog => {
          //① 定义RedisKey
          val redisKey = s"RealTime-StartUp-${starUpLog.logDate}"
          //② 获取Jedis连接
          val jedisClient: Jedis = MyRedisUtil.getJedis()
          //③ 写库操作
          jedisClient.sadd(redisKey,starUpLog.mid)
          //③ 关闭连接
          jedisClient.close()
        })
      })
    })
  }

  /**
   * 跨批次去重：将当前时间段内的登录用户数据，和存入Redis中的数据进行对比，去除已经登录的用户
   * @param objectDStream 当前时间段内，登录的用户数据集
   * @return 去重后的数据集
   */
  def cleanRepetitionByHistory(ssc:StreamingContext, objectDStream: DStream[StartUpLog]): DStream[StartUpLog] = {

    //1.从Redis数据库中拿取用户登录的数据
    //① 获取Redis的连接
    val jedisClient: Jedis = MyRedisUtil.getJedis()
    //② 从redis中获取数据
    val currentTime: Long = System.currentTimeMillis()
    //a.今天的数据
    val todayStartUpUser: util.Set[String] = jedisClient
      .smembers(s"RealTime-StartUp-" + sdf.format(new Date(currentTime)))
    //b.昨天的数据
    val yesterdayStartUpUser: util.Set[String] = jedisClient
      .smembers(s"RealTime-StartUp-" + sdf.format(new Date(currentTime - 24*60*60*1000)))
    //③ 存入广播变量
    val todayData: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(todayStartUpUser)
    val yesterdayData: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(yesterdayStartUpUser)
    //④ 关闭连接
    jedisClient.close()

    //2.遍历数据，过滤当天已经登录过的用户
    val filterDStream: DStream[StartUpLog] = objectDStream.transform(rdd =>{
      rdd.mapPartitions(iter =>{
        iter.filter(startUpLog => {
          if (startUpLog.logDate.equals(sdf.format(new Date(currentTime)))) {
            !startUpLog.mid.contains(todayData)
          } else {
            !startUpLog.mid.contains(yesterdayData)
          }
        })
      })
    })

    //3.返回过滤后的结果
    filterDStream
  }
}
