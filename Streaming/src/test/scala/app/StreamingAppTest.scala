package app

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.DStream

object StreamingAppTest {
	
	def main(args: Array[String]): Unit = {
		// 1. StreamingContext实例对象
		val ssc = StreamingContextUtils.getStreamingContext(this.getClass, 5)
		
		// 2. 消费Kafka数据
		val kafkaDStream: DStream[ConsumerRecord[String, String]] = StreamingContextUtils.consumerKafka(ssc, "search-log-topic")
		
		// 3. 打印控制台
		kafkaDStream.print(50)
		
		// 4. 启动流式应用
		ssc.start()
		ssc.awaitTermination()
		ssc.stop(stopSparkContext = true, stopGracefully = true)
	}
	
}
