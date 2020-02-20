import com.caihua.constants.GmallConstants
import com.caihua.utils.MyKafkaUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @author XiLinShiShan
 * @version 0.0.1
 */
object TestKafkaUtils {
  def main(args: Array[String]): Unit = {
    //1.创建SparkConf
    val conf = new SparkConf().setAppName("RealTime").setMaster("local[*]")
    //2.创建SparkStreamingContext
    val ssc = new StreamingContext(conf,Seconds(5))
    //3.读取Kafka中的数据，并创建DStream
     MyKafkaUtil.getKafkaDStream(ssc,Set(GmallConstants.KAFKA_TOPIC_EVENT)).print()
    //4.开启任务
    ssc.start()
    //阻塞main线程
    ssc.awaitTermination()

  }

}
