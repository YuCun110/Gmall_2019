package com.caihua.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.caihua.bean.StartUpLog
import com.caihua.constants.GmallConstants
import com.caihua.handler.{HbaseHandler, RedisHandler, RepetitionByBatchHandler}
import com.caihua.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object DauApp {

  //1.创建时间格式化对象
  private val sdf = new SimpleDateFormat("yyyy-MM-dd HH")

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf
    val conf = new SparkConf().setAppName("RealTime").setMaster("local[*]")

    //2.创建SparkStreamingContext
    val ssc = new StreamingContext(conf,Seconds(3))

    //3.读取Kafka中的数据，并创建DStream
    val jsonDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaDStream(ssc,Set(GmallConstants.KAFKA_TOPIC_STARTUP))

    //4.将原始JSON数据封装为StartUpLog对象
    val objectDStream: DStream[StartUpLog] = jsonDStream.map {
      case (_, json) => {
        //① 将JSON字符串转换为对象
        val startUpLog: StartUpLog = JSON.parseObject(json, classOf[StartUpLog])
        //② 时间格式化
        val date: String = sdf.format(new Date(startUpLog.dt))
        //③ 拆分日期和小时，添加到该对象中
        val arr: Array[String] = date.split(" ")
        startUpLog.logDate = arr(0)
        startUpLog.logHour = arr(1)
        //④ 返回
        startUpLog
      }
    }

    //5.跨批次去重，将当前时间段登录的设备id与存入Redis中的数据进行对比，去重
    val filterByHistoryDStream: DStream[StartUpLog] = RedisHandler.cleanRepetitionByHistory(ssc,objectDStream)

    //6.批次内去重，将当前时间段内的数据进行去重
    val filterByBatchDStream: DStream[StartUpLog] = RepetitionByBatchHandler.cleanRepetition(filterByHistoryDStream)
    //缓存数据
    filterByBatchDStream.cache()
    filterByBatchDStream.count().print()

    //7.将当前批次去重后的结果，存入Redis中
    RedisHandler.saveToRedis(filterByBatchDStream)

    //8.将当前批次去重后的结果，存入Hbase
    HbaseHandler.saveToHbase(filterByBatchDStream)

    //开启任务
    ssc.start()
    //阻塞main线程
    ssc.awaitTermination()
  }

}
