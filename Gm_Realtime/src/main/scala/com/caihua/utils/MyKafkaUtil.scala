package com.caihua.utils

import java.util.Properties

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object MyKafkaUtil {

  def getKafkaDStream(ssc:StreamingContext,topics:Set[String]):InputDStream[(String,String)] ={

    //1.从配置文件中读取Kafka配置参数
    val properties: Properties = MyPropertiesUtil.load("config.properties")
    val brokerList: String = properties.getProperty("kafka.broker.list")

    //2.封装为Map集合
    val kafkaParams: Map[String, String] = Map[String, String](
      "bootstrap.servers" -> brokerList,
      "group.id" -> "group04"
    )

    //3.从Kafka中读取指定Topic的数据，创建SparkStreamingContext
    KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)

  }

  def getKafkaProducer(bootstrap_servers:String): KafkaProducer[String,String] ={
    //① 创建Properties对象
    val properties = new Properties
    //② 设置参数
    //设置Kafka集群信息
    properties.put("bootstrap.servers", bootstrap_servers)
    properties.put("acks", "all")
    //序列化
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //③ 创建Producer对象
    new KafkaProducer[String, String](properties)
  }
}
