package shared

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 基于Spark框架使用Scala语言编程实现词频统计WordCount程序，将符号数据过滤，并统计出现的次数
	 *  -a. 过滤标点符号数据
	 *          使用广播变量
	 *  -b. 统计出标点符号数据出现次数
	 *         使用累加器
 */
object _05SparkSharedVariableTest {
	
	def main(args: Array[String]): Unit = {
		// 1. 在Spark 应用程序中，入口为：SparkContext，必须创建实例对象，加载数据和调度程序执行
		val sc: SparkContext = {
			// 创建SparkConf对象，设置应用相关信息，比如名称和master
			val sparkConf: SparkConf = new SparkConf()
				.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
				.setMaster("local[2]")
			// 构建SparkContext实例对象，传递SparkConf
			SparkContext.getOrCreate(sparkConf)
		}
		
		// 2. 第一步、从LocalFS读取文件数据，sc.textFile方法，将数据封装到RDD中
		val inputRDD: RDD[String] = sc.textFile("datas/filter/datas.input", minPartitions = 2)
		
		// TODO: 字典数据，只要有这些单词就过滤: 特殊字符存储列表List中
		val list: List[String] = List(",", ".", "!", "#", "$", "%")
		// TODO: 将字典数据进行广播变量
		val listBroadcast: Broadcast[List[String]] = sc.broadcast(list)
		
		// TODO: 定义计数器
		val accumulator: LongAccumulator = sc.longAccumulator("number_accu")
		
		// 3. 第二步、调用RDD中高阶函数，进行处理转换处理，函数：flapMap、map和reduceByKey
		val resultRDD: RDD[(String, Int)] = inputRDD
			// 过滤空行数据
			.filter(line => null != line && line.trim.length > 0)
			// 分割为单词
			.flatMap(line => line.trim.split("\\s+"))
			// TODO: 此处，应该判断"word"，如果是非单词字符，比如标点符号，应用过滤掉
    		.filter{word =>
			    // 获取广播变量的值
			    val valueList: List[String] = listBroadcast.value
			    // 判断单词是否为字符
			    val isContains: Boolean = valueList.contains(word)
			    if(isContains){
				    // 累加统计出现次数
				    accumulator.add(1L)
			    }
			    // 返回
			    !isContains
		    }
			// 按照单词分组，进行聚合操作
            .map(word => (word, 1))
			// 分组聚合
            .reduceByKey(_ + _)
		
		// 4. 第三步、将最终处理结果RDD保存到HDFS或打印控制台
		/*
			(hive,6)
			(mapreduce,4)
			(,,1)
			(sql,2)
			(spark,11)
			(hadoop,3)
			(#,2)
			(!,4)
			(%,1)
			(hdfs,2)
		 */
		//resultRDD.foreach(println)
		
		resultRDD.foreach(println)
		
		// TODO：获取累加器的值，必须使用RDD Action函数进行触发
		println(s"Accu = ${accumulator.value}")
		
		// 5. 当应用运行结束以后，关闭资源
		Thread.sleep(1000000000)
		sc.stop()
	}
	
}
