package kafka

import java.util

import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, ConsumerStrategy, KafkaUtils, LocationStrategies, LocationStrategy}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Streaming通过Kafka New Consumer消费者API获取数据
 */
object _05StreamingSourceKafka {
	
	def main(args: Array[String]): Unit = {
		
		// 1. 构建StreamingContext实例对象，传递时间间隔BatchInterval
		val ssc: StreamingContext = {
			// a. 创建SparkConf对象，设置应用基本信息
			val sparkConf = new SparkConf()
    			.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    			.setMaster("local[3]")
			// b. 创建实例对象，设置BatchInterval
			new StreamingContext(sparkConf, Seconds(5))
		}
		
		// 2. 定义数据源，获取流式数据，封装到DStream中
		// TODO: 从Kafka消费数据，采用New Consumer API方式
		/*
		  def createDirectStream[K, V](
		      ssc: StreamingContext,
		      locationStrategy: LocationStrategy,
		      consumerStrategy: ConsumerStrategy[K, V]
		    ): InputDStream[ConsumerRecord[K, V]]
		 */
		// a. 设置消费Kafka数据时，位置策略
		val locationStrategy: LocationStrategy = LocationStrategies.PreferConsistent
		// b. 设置消费Kafka数据时，消费策略，传递基本参数
		/*
		def Subscribe[K, V](
		      topics: Iterable[jl.String],
		      kafkaParams: collection.Map[String, Object]
		 ): ConsumerStrategy[K, V]
		 */
		val kafkaParams = Map[String, Object](
			"bootstrap.servers" -> "node1.itcast.cn:9092",
			"key.deserializer" -> classOf[StringDeserializer],
			"value.deserializer" -> classOf[StringDeserializer],
			"group.id" -> "group_id_1001",
			"auto.offset.reset" -> "latest",
			"enable.auto.commit" -> (false: java.lang.Boolean)
		)
		val consumerStrategy: ConsumerStrategy[String, String] = ConsumerStrategies.Subscribe(
			Array("wc-topic"), kafkaParams
		)
		// c. 采用new ConsumerAPI 消费Kafka topic数据
		val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
			ssc, //
			locationStrategy, //
			consumerStrategy //
		)
		// 仅仅获取Kafka Topic中Value数据：Message消息
		val inputDStream: DStream[String] = kafkaDStream.map(record => record.value())
		
		// 3. 依据业务需求，调用DStream中转换函数（类似RDD中转换函数）
		/*
			def transform[U: ClassTag](transformFunc: RDD[T] => RDD[U]): DStream[U]
		 */
		// 此处rdd就是DStream中每批次RDD数据
		val resultDStream: DStream[(String, Int)] = inputDStream.transform{ rdd =>
			rdd
				.filter(line => null != line && line.trim.length > 0)
				.flatMap(line => line.trim.split("\\s+"))
				.map(word => (word, 1))
				.reduceByKey((tmp, item) => tmp + item)
		}
		
		// 4. 定义数据终端，将每批次结果数据进行输出
		/*
			def foreachRDD(foreachFunc: (RDD[T], Time) => Unit): Unit
		 */
		resultDStream.foreachRDD((rdd, time) => {
			//val xx: Time = time
			val format: FastDateFormat = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss")
			println("-------------------------------------------")
			println(s"Time: ${format.format(time.milliseconds)}")
			println("-------------------------------------------")
			// 判断每批次结果RDD是否有数据，如果有数据，再进行输出
			if(!rdd.isEmpty()){
				rdd.coalesce(1).foreachPartition(iter => iter.foreach(println))
			}
		})
		
		// 5. 启动流式应用，等待终止
		ssc.start()
		ssc.awaitTermination()
		ssc.stop(stopSparkContext = true, stopGracefully = true)
	}
	
}
