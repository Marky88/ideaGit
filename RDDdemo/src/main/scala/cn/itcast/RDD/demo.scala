package cn.itcast.RDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object demo {
  /**
   * 将RDD数据保存至MySQL表中: tb_wordcount
   */


  def main(args: Array[String]): Unit = {
    // 1. 在Spark 应用程序中，入口为：SparkContext，必须创建实例对象，加载数据和调度程序执行
    val sc: SparkContext = {
      // 创建SparkConf对象，设置应用相关信息，比如名称和master
      val sparkConf: SparkConf = new SparkConf()
        .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
        .setMaster("local[2]")
      // 构建SparkContext实例对象，传递SparkConf
      new SparkContext(sparkConf)
    }
    // 2. 第一步、从LocalFS读取文件数据，sc.textFile方法，将数据封装到RDD中
    val inputRDD: RDD[String] = sc.textFile("file:///E:\\Project\\Spark\\datas\\input\\wordcount.txt")

    // 3. 第二步、调用RDD中高阶函数，进行处理转换处理，函数：flapMap、map和reduceByKey
    val resultRDD: RDD[(String, Int)] = inputRDD
      // a. 过滤
      .filter(line => null != line && line.trim.length > 0)
      // b. 对每行数据按照分割符分割
      .flatMap(line => line.trim.split("\\s+"))
      // c. 将每个单词转换为二元组，表示出现一次
      .map(word => (word, 1))
      // d. 分组聚合
      .reduceByKey((temp, item) => temp + item)

    resultRDD.saveAsTextFile(s"/hi-${System.currentTimeMillis()}")
   // resultRDD.foreach(println)



    // 4. 第三步、将最终处理结果RDD保存到HDFS或打印控制台
    /*
       (spark,11)
       (hive,6)
       (hadoop,3)
       (mapreduce,4)
       (hdfs,2)
       (sql,2)
       */
    //resultRDD.foreach(tuple => println(tuple))


    //
    //  resultRDD.coalesce(1)
    //     .foreachPartition{saveToMySQL}
    //  // TODO: 将结果数据resultRDD保存至MySQL表中
    //  /*
    //   a.对结果数据降低分区数目
    //   b.针对每个分区数据进行操作
    //    每个分区数据插入数据库时，创建一个连接Connection
    //   */
    //
    //  // 5. 当应用运行结束以后，关闭资源
    //  sc.stop()

  }
}