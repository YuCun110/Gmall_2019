package com.caihua.bean

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
case class EventLog(
       mid:String,
       uid:String,
       appid:String,
       area:String,
       os:String,
       `type`:String,
       evid:String,
       pgid:String,
       npgid:String,
       itemid:String,
          var logDate:String,
          var logHour:String,
          var dt:Long
     )
