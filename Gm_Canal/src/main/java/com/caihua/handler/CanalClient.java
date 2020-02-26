package com.caihua.handler;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.caihua.constants.GmallConstants;
import com.caihua.utils.MyKafkaUtil;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1.创建CanalConnector连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop202", 11111),
                "example", "", "");

        //2.抓取数据并解析
        while (true) {
            //① 连接Canal
            canalConnector.connect();
            //② 指定订阅的数据库
            canalConnector.subscribe("gmall.*");
            //③ 抓取数据
            Message message = canalConnector.get(100);
            //④ 取出Entry集合
            List<CanalEntry.Entry> entries = message.getEntries();
            //⑤ 判断集合是否有效
            if (entries.size() <= 0) {
                System.out.println("暂无数据，休息片刻！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else {
                //对有效数据，取出Entry集合并遍历
                for (CanalEntry.Entry entry : entries) {
                    //a.筛选当前数据的类型，只留下MySQL对数据操作（ROWDATA）的内容
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {
                        //反序列化数据的结果集
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        //获取当前数据的表名
                        String tableName = entry.getHeader().getTableName();
                        //取出MySQL操作当前数据的类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        //根据操作类型筛选数据，并将结果发送到Kafka
                        kafkaHandler(tableName, eventType,rowChange);
                    }
                }
            }
        }
    }

    /**
     * 创建KafkaProducer，将MySQL中insert操作的结果集数据，存入Kafka集群中
     * @param tableName　表名
     * @param eventType　MySQL操作的类型
     * @param rowChange　数据集
     */
    private static void kafkaHandler(String tableName, CanalEntry.EventType eventType, CanalEntry.RowChange rowChange) {
        //1.创建KafkaProducer
        KafkaProducer<String, String> kafkaProducer = MyKafkaUtil.getKafkaProducer("hadoop202:9092,hadoop203:9092,hadoop204:9092");

        //2.选择不同的Topic主题
        String topic = "";
        if("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){
            //topic = GmallConstants.KAFKA_TOPIC_ORDER;
            saveToKafka(rowChange,kafkaProducer,GmallConstants.KAFKA_TOPIC_ORDER);
        }else if("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){
            //topic = GmallConstants.KAFKA_TOPIC_ORDER_DETAIL;
            saveToKafka(rowChange,kafkaProducer,GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);
        }else if("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))){
            //topic = GmallConstants.KAFKA_TOPIC_USER_INFO;
            saveToKafka(rowChange,kafkaProducer,GmallConstants.KAFKA_TOPIC_USER_INFO);
        }

        //3.将数据存入Kafka中
        //saveToKafka(rowChange,kafkaProducer,topic);
    }

    /**
     * 遍历结果集中的行元素数据，并将数据写入Kafka集群
     * @param rowChange Canal读取的数据集
     * @param kafkaProducer Kafka生产者对象
     * @param topic Kafka的主题
     */
    private static void saveToKafka(CanalEntry.RowChange rowChange, KafkaProducer<String, String> kafkaProducer, String topic) {
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            //① 创建JSON对象，用于存放数据
            JSONObject jsonObject = new JSONObject();
            //② 读取行数据中的字段
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                //存入JSON中
                jsonObject.put(column.getName(),column.getValue());
            }
            System.out.println(jsonObject.toString());

            //③ 将JSON字符串发送至Kafka集群中
            kafkaProducer.send(new ProducerRecord<>(topic,jsonObject.toString()));
        }
    }
}
