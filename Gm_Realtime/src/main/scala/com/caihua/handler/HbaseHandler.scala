package com.caihua.handler

import com.caihua.bean.StartUpLog
import org.apache.hadoop.conf.Configuration
import org.apache.spark.streaming.dstream.DStream

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object HbaseHandler {
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
