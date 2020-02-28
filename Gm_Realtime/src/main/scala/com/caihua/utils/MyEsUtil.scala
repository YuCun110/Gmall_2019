package com.caihua.utils

import java.util.Objects
import java.util

import com.caihua.bean.CouponAlertLog
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.core.{Bulk, BulkResult, Index}

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object MyEsUtil {
  //1.定义ES连接地址
  private val httpUrl = "http://hadoop202:9200"
  //2.定义ES客户端工厂对象
  private var jestClientFactory:JestClientFactory = null

  /**
   * @return 返回ES客户端工厂对象
   */
  private def build(): Unit ={
    //1.创建ES客户端工厂对象
    jestClientFactory = new JestClientFactory()

    //2.创建ES连接地址对象
    val httpClientConfig = new HttpClientConfig.Builder(httpUrl)
      .multiThreaded(true)
      .maxTotalConnection(20) //连接总数
      .connTimeout(10000)
      .readTimeout(10000)
      .build()

    //3.设置ES连接地址
    jestClientFactory.setHttpClientConfig(httpClientConfig)
  }

  /**
   * @return 获取ES客户端对象
   */
  private def getESClient(): JestClient ={
    //1.如果ES客户端工厂对象为空，则创建
    if(jestClientFactory == null){
      build()
    }
    //2.返回ES客户端对象
    jestClientFactory.getObject
  }

  /**
   * 批量插入数据
   * @param indexName
   * @param data
   */
  def insertES(indexName:String,data: List[(String,Any)]): Unit ={
    //1.判断数据是否为空
    if(data.nonEmpty){
      //① 获取ES客户端对象
      val jestClient: JestClient = getESClient()
      //② 创建Bulk对象
      val bulkBuilder: Bulk.Builder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
      //③ 遍历数据集，批量存储
      for ((id,any) <- data) {
        //a.创建Index对象
        val indexBuilder = new Index.Builder(any)
        //b.添加Id
        if(id != null){
          indexBuilder.id(id)
        }
        //c.获取Index对象
        val index: Index = indexBuilder.build()
        //d.在Bulk中添加对象
        bulkBuilder.addAction(index)
      }

      //④ 获取Bulk对象
      val bulk: Bulk = bulkBuilder.build()
      //⑤ 创建接收执行保存数据的对象
      var items: util.List[BulkResult#BulkResultItem] = null
      //⑥ 执行写入操作
      try{
        items = jestClient.execute(bulk).getItems
      }catch {
        case e:Exception => println(e.toString)
      }finally {
        //a.关闭连接
        close(jestClient)
        //b.打印执行的结果信息
        println("保存" + items.size() + "条数据")
        import scala.collection.JavaConversions._
//        for(item <- items){
//          println(item.error)
//          println(item.errorReason)
//        }
      }
    }
  }

  /**
 * 关闭ES客户端连接
 * @param client
 */
  def close(client: JestClient): Unit ={
    if(!Objects.isNull(client)){
      client.shutdownClient()
    }
  }
}
