package com.caihua.handler

import com.caihua.bean.StartUpLog
import org.apache.spark.streaming.dstream.DStream

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object RepetitionByBatchHandler {
  /**
   * 将跨批次去重后的数据集，对本批次内部的数据进行去重
   * @param filterByHistoryDStream 跨批次去重后的数据集
   * @return 跨批次和本批次去重后的数据集
   */
  def cleanRepetition(filterByHistoryDStream: DStream[StartUpLog]): DStream[StartUpLog] = {

    //1.变换数据结构：(startUpLog) -> ((date,mid),startUpLog)
    val tupleDStream: DStream[((String, String), StartUpLog)] = filterByHistoryDStream.map(startUpLog => {
      ((startUpLog.logDate, startUpLog.mid), startUpLog)
    })

    //2.根据日期和设备id，对数据集进行分组，选取最早的登录信息
    val filterDStream: DStream[StartUpLog] = tupleDStream.groupByKey().flatMap {
      case (tuple, iter) => {
        //按照登录时间，对startUpLog对象进行排序，选取最早的登录信息
        iter.toList.sortWith(_.dt < _.dt).take(1)
      }
    }

    //3.返回过滤后的数据集
    filterDStream
  }
}
