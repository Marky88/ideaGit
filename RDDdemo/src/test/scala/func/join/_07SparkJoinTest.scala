package func.join

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * RDD中关联函数Join，针对RDD中数据类型为Key/Value对
 */
object _07SparkJoinTest {
	
	def main(args: Array[String]): Unit = {
		// 创建SparkContext实例对象，传递SparkConf对象，设置应用配置信息
		val sc: SparkContext = {
			// a. 创建SparkConf对象
			val sparkConf = new SparkConf()
				.setAppName(this.getClass.getSimpleName.stripSuffix("$"))
				.setMaster("local[2]")
			// b. 传递sparkConf对象，构建SparkContext实例
			SparkContext.getOrCreate(sparkConf)
		}
		
		// 模拟数据集
		val empRDD: RDD[(Int, String)] = sc.parallelize(
			//   deptno   ename
			Seq((1001, "zhangsan"), (1002, "lisi"), (1003, "wangwu"), (1004, "zhaoliu"))
		)
		val deptRDD: RDD[(Int, String)] = sc.parallelize(
			//  deptno   dname
			Seq((1001, "sales"), (1002, "tech"))
		)
		
		// TODO: 等值连接
		/*
			def join[W](other: RDD[(K, W)]): RDD[   (  K,  (V, W)   )    ]
		 */
		empRDD
			.join(deptRDD) // 按照Key进行等值关联
			.foreach(tuple => {
				val key = tuple._1 // 关联Key
				val v1 = tuple._2._1 // ename
				val v2 = tuple._2._2 // dname
				//println(s"deptno = ${key}, ename = ${v1}, dname = ${v2}")
			})
		// TODO: 采用如下方式书写代码，利用模式匹配，偏函数
		empRDD
			.join(deptRDD)
			.foreach{case (deptno, (ename, dname)) =>
				println(s"deptno = ${deptno}, ename = ${ename}, dname = ${dname}")
			}
		
		println("======================================================")
		// TODO：左外连接
		/*
		    def leftOuterJoin[W](other: RDD[(K, W)]): RDD[(K, (V, Option[W]))]
		 */
		empRDD
			.leftOuterJoin(deptRDD)
			.foreach{case (deptno, (ename, option)) =>
				val dname = option.getOrElse("未知")
				println(s"deptno = ${deptno}, ename = ${ename}, dname = ${dname}")
			}
		
		
		// 应用结束，关闭资源
		sc.stop()
	}
	
}
