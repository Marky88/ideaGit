package search

import java.util

import com.hankcs.hanlp.HanLP
import com.hankcs.hanlp.seg.common.Term
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

/**
 * 用户查询日志(SogouQ)分析，数据来源Sogou搜索引擎部分网页查询需求及用户点击情况的网页查询日志数据集合。
	 * 1. 搜索关键词统计，使用HanLP中文分词
	 * 2. 用户搜索次数统计
	 * 3. 搜索时间段统计
 * 数据格式：
 *      访问时间\t用户ID\t[查询词]\t该URL在返回结果中的排名\t用户点击的顺序号\t用户点击的URL
 * 其中，用户ID是根据用户使用浏览器访问搜索引擎时的Cookie信息自动赋值，即同一次使用浏览器输入的不同查询对应同一个用户ID
 **/
object _01SogouQueryAnalysis {
	
	def main(args: Array[String]): Unit = {
		// 在Spark 应用程序中，入口为：SparkContext，必须创建实例对象，加载数据和调度程序执行
		val sc: SparkContext = {
			// 创建SparkConf对象
			val sparkConf: SparkConf = new SparkConf()
    			.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    			.setMaster("local[2]")
			// 构建SparkContext实例对象
			SparkContext.getOrCreate(sparkConf)
		}
		
		// TODO: 1. 从本地文件系统读取搜索日志数据
		//val sogouRDD: RDD[String] = sc.textFile("datas/sogou/SogouQ.sample", minPartitions = 2)
		val sogouRDD: RDD[String] = sc.textFile("datas/sogou/SogouQ.reduced", minPartitions = 2)
		//println(s"Count = ${sogouRDD.count()}")
		//println(s"First:\n\t${sogouRDD.first()}")
		
		// TODO: 2. 解析数据（先过滤不合格的数据），封装样例类SogouRecord对象
		val recordRDD: RDD[SogouRecord] = sogouRDD
			// 过滤非法数据
			.filter(line => null != line && line.trim.split("\\s+").length == 6)
			// 解析每条数据，封装到样例类对象
			.map(line => {
				// 分割日志数据
				val array: Array[String] = line.trim.split("\\s+")
				// 封装对象
				val record: SogouRecord = SogouRecord(
					array(0), array(1), //
					array(2).replaceAll("\\[", "").replace("]", ""), //
					array(3).toInt, array(4).toInt, array(5) //
				)
				// 返回实例
				record
			})
		//println(s"Count = ${recordRDD.count()}")
		//recordRDD.take(10).foreach(println)
		
		// TODO: 考虑到后续对解析封装RDD进行多次使用，所以进行持久化缓存
		recordRDD.persist(StorageLevel.MEMORY_AND_DISK)
		recordRDD.count()
		
		// TODO: 3. 依据需求对数据进行分析
		/*
			需求一、搜索关键词统计，使用HanLP中文分词
				- 第一步、获取每条日志数据中【查询词`queryWords`】字段数据
				- 第二步、使用HanLP对查询词进行中文分词
				- 第三步、按照分词中单词进行词频统计，类似WordCount
		 */
		val top10QueryWords: Array[(String, Int)] = recordRDD
			// 获取查询词，并且分词
			.flatMap(record => {
				// 第一步、获取每条日志数据中【查询词`queryWords`】字段数据
				val queryWords: String = record.queryWords
				// 第二步、使用HanLP对查询词进行中文分词
				val terms: util.List[Term] = HanLP.segment(queryWords.trim)
				// 转换Scala集合实例对象
				import scala.collection.JavaConverters._
				val words: mutable.Seq[String] =terms.asScala.map(term => term.word.trim)
				// 返回分割单词
				words.toList
			})
			// 第三步、按照分词中单词进行词频统计，类似WordCount
			.map(word => (word, 1))
			// 按照单词分组和聚合
			.reduceByKey(_ + _)
			// 按照统计词频降序排序
			.sortBy(tuple => tuple._2, ascending = false)
			// 获取前10个搜索次数最多的单词
			.take(10)
		top10QueryWords.foreach(println)
		
		/*
			需求二、用户搜索次数统计
				TODO： 统计每个用户对每个搜索词的点击次数，二维分组：先对用户分组，再对搜索词分组
			SQL:
				SELECT user_id, query_words, COUNT(1) AS total FROM records GROUP BY user_id, query_words
		 */
		val preUserQueryCountRDD: RDD[((String, String), Int)] = recordRDD
			// 提取字段，并且标识1词
			.map(record => {
				// 用户ID
				val userId = record.userId
				// 搜索查询词
				val queryWords: String = record.queryWords
				// 封装二元组，表示此用户针对该搜索词结果点击1词
				((userId, queryWords), 1)
			})
			// 按照key（用户和插叙词）进行分组和统计
			.reduceByKey((tmp, item) => tmp + item)
		preUserQueryCountRDD.take(50).foreach(println)
		
		// 获取次数即可
		val countRDD: RDD[Int] = preUserQueryCountRDD.map(_._2)
		println(s"Max Click Count: ${countRDD.max()}")
		println(s"Min Click Count: ${countRDD.min()}")
		println(s"Mean Click Count: ${countRDD.mean()}")
		
		
		println("=================================================")
		/*
			需求三、搜索时间段统计, 按照每个小时统计用户搜索次数
				00:00:00  -> 00  提取出小时
		 */
		val top24Query: Array[(String, Int)] = recordRDD
			// 提取搜索时间，获取小时，转换 二元组，表示该小时被搜索1词
			.map{record =>
				// 提取搜索时间
				val queryTime: String = record.queryTime
				// 获取小时
				val hour: String = queryTime.substring(0, 2)
				// 封装二元组
				(hour, 1)
			}
			// 按照小时分组和聚合
			.reduceByKey((tmp, item) => tmp + item)
			// 降序排序
			.top(24)(Ordering.by(tuple => tuple._2))
		top24Query.foreach(println)
		
		// 当数据不再使用时，释放资源
		recordRDD.unpersist()
		
		// 当应用运行结束以后，关闭资源
		Thread.sleep(10000000)
		sc.stop()
	}
	
}
