package com.caihua.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.caihua.bean.{CouponAlertLog, EventLog}
import com.caihua.constants.GmallConstants
import com.caihua.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scala.util.control.Breaks._

/**
 * @author XiLinShiShan
 * @version 0.0.1
 *          预警需求：
 *          同一设备，5分钟（30秒）内三次及以上用不同账号登录并领取优惠劵，并且在登录到领劵过程中没有浏览商品。达到以上要求则产生一条预警日志。
 *          同一设备，每分钟（5秒）只记录一次预警。
 */
object AlterApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf: SparkConf = new SparkConf().setAppName("AlterApp").setMaster("local[*]")

    //2.创建SparkStreamContext
    val ssc = new StreamingContext(conf, Seconds(5))

    //3.从Kafka中读取数据，创建DStream
    val kafkaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaDStream(ssc, Set(GmallConstants.KAFKA_TOPIC_EVENT))

    //4.开窗（30秒），将读取的用户行为数据封装为样例类
    val eventLogDStream: DStream[EventLog] = kafkaDStream.window(Seconds(30))
      .map {
        case (_, json) => {
          //① 将JSON字符串转换为样例类对象
          val eventLog: EventLog = JSON.parseObject(json, classOf[EventLog])
          //② 将日期，和小时字段补充完整
          //时间格式化
          val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
          val date: String = sdf.format(new Date(eventLog.dt))
          //补充字段
          eventLog.logDate = date.split(" ")(0)
          eventLog.logHour = date.split(" ")(1)
          //③ 返回结果
          eventLog
        }
      }

    //5.按照设备ID对数据进行分组
    val groupByMidDStream: DStream[(String, Iterable[EventLog])] = eventLogDStream.map(eventLog => (eventLog.mid, eventLog))
      .groupByKey()

    //6.筛选相同设备ID中，用不同用户ID登录三次及以上并领取优惠劵，且没有浏览商品的用户，并将其封装为预警日志样例类
    val filterDStream: DStream[(Boolean, CouponAlertLog)] = groupByMidDStream.map {
      case (mid, iter) => {
        //① 定义容器
        //存放同一设备ID，的不同用户ID信息
        val userIds = new java.util.HashSet[String]()
        //存放同一设备ID，浏览的商品明细
        val itemsIds = new java.util.HashSet[String]()
        //存放用户行为类型
        val events = new util.ArrayList[String]()
        //② 定义一个标记：用户是否浏览商品（true :默认没有浏览）
        var isFlag = true
        //③ 根据用户行为筛选
        breakable{
          iter.foreach(eventLog => {
            //如果用户浏览了商品信息
            if (eventLog.evid.equals("clickItem")) {
              isFlag = false
              break()
            } else if (eventLog.evid.equals("coupon")) {
              userIds.add(eventLog.uid)
              itemsIds.add(eventLog.itemid)
              events.add(eventLog.evid)
            }
          })
        }
        //④ 将可能的预警日志封装为样例类对象
        (userIds.size() >= 3 && isFlag, CouponAlertLog(mid, userIds, itemsIds, events, System.currentTimeMillis()))
      }
    }

    //7.判断是否满足预警条件，并将结果存入ES中
    filterDStream.filter(_._1).print()



    //开始任务
    ssc.start()
    //阻塞main线程
    ssc.awaitTermination()
  }
}
