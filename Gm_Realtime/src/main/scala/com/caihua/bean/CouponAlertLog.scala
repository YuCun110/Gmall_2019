package com.caihua.bean

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
case class CouponAlertLog(
       mid:String,
       uids:java.util.HashSet[String],
       itemIds:java.util.HashSet[String],
       events:java.util.List[String],
       ts:Long
     )
