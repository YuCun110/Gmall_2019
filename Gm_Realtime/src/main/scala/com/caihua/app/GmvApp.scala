package com.caihua.app

import com.alibaba.fastjson.JSON
import com.caihua.bean.OrderInfo
import com.caihua.handler.HbaseHandler
import com.caihua.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author XiLinShiShan
 * @version 0.0.1
 * 统计交易额
 */
object GmvApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("GMVApp").setMaster("local[*]")

    //2.创建SparkStreamingContext
    val ssc = new StreamingContext(conf,Seconds(3))

    //3.从Kafka中读取数据，创建DStream
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaDStream(ssc,Set("Gmall_Order"))

    //4.将数据集封装为样例类
    val objectDSTream: DStream[OrderInfo] = kafkaDStream.map {
      case (_,json) => {
        //① 将JSON字符串转化为样例类对象
        val orderInfo: OrderInfo = JSON.parseObject(json, classOf[OrderInfo])
        //② 处理时间
        val arr: Array[String] = orderInfo.create_time.split(" ")
        println(arr.mkString(","))
        orderInfo.create_date = arr(0)
        orderInfo.create_hour = arr(1).split(":")(0)
        //③ 手机号脱敏
        val tel: String = orderInfo.consignee_tel
        orderInfo.consignee_tel = tel.splitAt(3)._1 + "****" + tel.splitAt(7)._2
        //④ 返回
        orderInfo
      }
    }
    //5.将数据存储到Hbase(Phoenix)中
    HbaseHandler.saveToHbase02(objectDSTream)

    //开启任务
    ssc.start()
    //阻塞main线程
    ssc.awaitTermination()
  }
}
