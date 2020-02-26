package com.caihua.app

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.caihua.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.caihua.constants.GmallConstants
import com.caihua.utils.{MyKafkaUtil, MyRedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.{DefaultFormats, Formats}
import org.json4s.jackson.Serialization
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer


/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object SaleApp {
  def main(args: Array[String]): Unit = {
    //1.创建SparkCon
    val conf: SparkConf = new SparkConf().setAppName("SaleApp").setMaster("local[*]")

    //2.创建SparkStreamingContext
    val ssc = new StreamingContext(conf,Seconds(3))

    //3.从Kafka中读取三个主题中的数据，并分别创建DStream
    val oIDstaDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaDStream(ssc,Set(GmallConstants.KAFKA_TOPIC_ORDER))
    val oDDataDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaDStream(ssc,Set(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL))
    val uIDataDStream: InputDStream[(String, String)] = MyKafkaUtil.getKafkaDStream(ssc,Set(GmallConstants.KAFKA_TOPIC_USER_INFO))

    //4.将读取的数据转换为样例类对象，并且转换数据类型：-> (order_id,object)
    val orderInfoDStream: DStream[(String, OrderInfo)] = oIDstaDStream.map {
      case (_, data) => {
        //① 将JSON字符串转换为样例类对象
        val orderInfo: OrderInfo = JSON.parseObject(data, classOf[OrderInfo])
        //② 补全时间信息
        val arr: Array[String] = orderInfo.create_time.split(" ")
        orderInfo.create_date = arr(0)
        orderInfo.create_hour = arr(1).split(":")(0)
        //③ 手机号脱敏处理
        orderInfo.consignee_tel = orderInfo.consignee_tel.splitAt(3)._1 + "****" + orderInfo.consignee_tel.splitAt(7)
        //④ 返回样例类对象
        orderInfo
      }
    }.map(orderInfo => (orderInfo.id, orderInfo))

    val orderDetailDStream: DStream[(String, OrderDetail)] = oDDataDStream.map {
      case (_, data) => JSON.parseObject(data, classOf[OrderDetail])
    }.map(orderDetail => (orderDetail.order_id, orderDetail))

    //5.将订单信息和订单详情Join
    val joinByInfoWithDetailDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoDStream.fullOuterJoin(orderDetailDStream)

    //6.对Join后的结果集进行批量处理
    val resultJoin01: DStream[SaleDetail] = joinByInfoWithDetailDStream.mapPartitions {
      iter => {
        //① 定义集合，存放待批量处理的数据
        val list = new ListBuffer[SaleDetail]()
        //② 获取Redis连接
        val jedisClient: Jedis = MyRedisUtil.getJedis()
        //隐式转换
        implicit val format: DefaultFormats.type = org.json4s.DefaultFormats

        //③ 遍历分区的每一个元素
        iter.foreach {
          case (orderId, (orderInfoOp, orderDetailOp)) => {
            //Ⅰ.设置Redis中的Key
            val orderInfoKey: String = s"order_info_$orderId"
            val orderDetailKey: String = s"order_detail_$orderId"

            //Ⅱ.判断左边是否为空
            if (orderInfoOp.isDefined) {
              //取出orderInfoOp中的数据
              val orderInfoData: OrderInfo = orderInfoOp.get

              //a.判断右边是否为空
              if (orderDetailOp.isDefined) {
                //取出orderDetailOp中的数据
                val orderDetailData: OrderDetail = orderDetailOp.get

                //左右两边数据均存在，Join成功：将结果封装为SaleDetail对象，并将结果放在容器中
                list.append(new SaleDetail(orderInfoData, orderDetailData))
              }

              //b.将orderInfo转换为JSON字符串，并写入Redis中，已便后续Join
              val orderInfoJson: String = Serialization.write(orderInfoData)
              jedisClient.set(orderInfoKey, orderInfoJson)
              //设置有效时间
              jedisClient.expire(orderInfoKey, 300)

              //c.查询缓存中的orderDetail信息
              val detailExist: util.Set[String] = jedisClient.smembers(orderDetailKey)
              //导入隐式转换
              import scala.collection.JavaConversions._

              //遍历查询的结果，存在则和orderInfoData组合成Join后的结果，放入容器中
              detailExist.foreach(detail => {
                //将JSON字符串转换为orderDetail对象
                val orderDetail: OrderDetail = JSON.parseObject(detail, classOf[OrderDetail])
                //放入容器中
                list.append(new SaleDetail(orderInfoData, orderDetail))
              })
              //Ⅲ.左边为空
            } else {
              //a.获取右边的数据
              val orderDetail: OrderDetail = orderDetailOp.get

              //b.查看缓存中对应的orderInfo是否存在
              if (jedisClient.exists(orderInfoKey)) {
                //左边为空，右边不为空，可以与缓存Join
                //读取缓存中的orderInfo数据，并转换为orderInfo对象
                val orderInfo: OrderInfo = JSON.parseObject(jedisClient.get(orderInfoKey), classOf[OrderInfo])
                ////放入容器中
                list.append(new SaleDetail(orderInfo, orderDetail))
              } else {
                //缓存中对应的数据不存在，将orderDetail写入Redis中
                jedisClient.sadd(orderDetailKey, Serialization.write(orderDetail))
                //设置有效时间
                jedisClient.expire(orderDetailKey, 300)
              }
            }
          }
        }
        //④ 关闭RedisClient连接
        jedisClient.close()
        //⑤ 返回结果
        list.toIterator
      }
    }
    resultJoin01.print(100)

    //启动任务
    ssc.start()
    //阻塞main线程
    ssc.awaitTermination()
  }
}
