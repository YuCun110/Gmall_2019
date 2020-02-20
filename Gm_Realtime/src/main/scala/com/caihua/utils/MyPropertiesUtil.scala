package com.caihua.utils

import java.io.{InputStream, InputStreamReader}
import java.util.Properties


/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object MyPropertiesUtil {
  def load(propertiesPath: String): Properties = {
    //1.创建Properties对象
    val properties = new Properties()
    //2.创建输出流
    val properInputStream: InputStream = Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesPath)
    //3.读取配置文件中的数据
    properties.load(new InputStreamReader(properInputStream,"UTF-8"))
    //4.返回
    properties

  }
}
