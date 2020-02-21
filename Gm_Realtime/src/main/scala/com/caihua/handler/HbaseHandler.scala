package com.caihua.handler

import com.caihua.bean.{OrderInfo, StartUpLog}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.streaming.dstream.DStream

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object HbaseHandler {
  /**
   * 将Order_Info对象的DStream存入Hbase中
   * @param objectDSTream
   */
  def saveToHbase02(objectDSTream: DStream[OrderInfo]) = {
    objectDSTream.foreachRDD(rdd => {
      //1.导包
      import org.apache.phoenix.spark._
      //2.存入Phoenix中的GMALL_2019_ORDER_INFO表中
      rdd.saveToPhoenix("GMALL_2019_ORDER_INFO",
        Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME", "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        new Configuration(), Some("hadoop202,hadoop203,hadoop204:2181"))
    })
  }

  /**
   *
   * @param filterByBatchDStream
   */
  def saveToHbase(filterByBatchDStream: DStream[StartUpLog]) = {
    filterByBatchDStream.foreachRDD(rdd => {
      //1.导包
      import org.apache.phoenix.spark._
      //2.存入Phoenix中的GMALL_2019_DAU表中
      rdd.saveToPhoenix("GMALL_2019_DAU",
        Seq("MID","UID","APPID","AREA","OS","CH","TYPE","VS","LOGDATE","LOGHOUR","DT"),
        new Configuration,Some("hadoop202,hadoop203,hadoop204:2181"))
    })
  }

}
