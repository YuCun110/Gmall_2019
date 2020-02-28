package com.caihua.constants;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class GmallConstants {
    //Kafka用户启动日志主题
    public static final String KAFKA_TOPIC_STARTUP="Gmall_Startup";
    //Kafka用户事件日志主题
    public static final String KAFKA_TOPIC_EVENT="Gmall_Event";
    //Kafka统业务中的订单主题
    public static final String KAFKA_TOPIC_ORDER="Gmall_Order";
    //Kafka系统业务数据的订单详情主题
    public static final String KAFKA_TOPIC_ORDER_DETAIL="Gmall_Order_Detail";
    //Kafka系统业务数据的用户主题
    public static final String KAFKA_TOPIC_USER_INFO="Gmall_User";
    //ES中恶意领券黑名单的Index名称
    public static final String ES_INDEX_COUPON_ALTER="gmall_coupon_alert";
    //ES中销售详情的Index名称
    public static final String ES_INDEX_SALE_DETAIL="gmall19_sale_detail";

}
