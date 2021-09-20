package cn.itcast.log.search

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object SougouSearch {
    val sc :SparkContext ={
      val conf: SparkConf = new SparkConf()
                            .setMaster("local[2]")
                            .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
    SparkContext.getOrCreate(conf)
    }

  def main(args: Array[String]): Unit = {
    val input: RDD[String] = sc.textFile("datas/input/SogouQ.sample",minPartitions=3)   //得到每一行的内容
    // 解析数据（先过滤不合格的数据），封装样例类SogouRecord对象
    val sougouRDD: RDD[SougouRecord] = input.filter(log => (null != log) && log.trim.split("\\s+").length == 6) //过滤掉log不为空 并且去掉头尾的空格的长度等于6
      // .flatMap(lines => lines.trim.split("\\s+"))
      // 解析日志，封装实例对象
      .mapPartitions(iter => //iter 迭代器 代表每个分区
        iter.map(log => //log代表每行数据
        {
          val split: Array[String] = log.trim.split("\\s+") //将每行的数据封装到一个SougouRecord中
          SougouRecord(
            split(0),
            split(1),
            split(2),
            split(3).toInt,
            split(4).toInt,
            split(5)
          )
        }
        )
      )

    // println(s"count=${sougouRDD.count()}")
    // println(s"First =${sougouRDD.first()}")

    //todo 统计出每个用户每个搜索词点击网页的次数
    val clickCountRDD: RDD[((String, String), Int)] = sougouRDD   //按照(record.userId, record.queryWords)进行分组聚合
                          .map(record => ((record.userId, record.queryWords), 1))
                          .aggregateByKey(0)(
                            (temp, item) => temp + item,
                            (tp,im) =>tp+im
                          )
   // clickCountRDD.foreachPartition(clickcount =>)
 //  val countRDD: RDD[Int] = clickCountRDD.map(cd => cd._2)
    clickCountRDD.foreachPartition(countRDD => {      //3个分区
      println("======================")               //会出现两行======
      val partitionId: Int = TaskContext.getPartitionId()
      println(partitionId)
      countRDD.foreach( li=>
            println(s"Id:$partitionId: ${li._1}, count=${li._2}"))
    }

    )


/*
    //todo 按照【访问时间】字段获取【小时】，分组统计各个小时段用户查询搜索的数量
    val hourCount: RDD[(Int, Int)] = sougouRDD.mapPartitions(record => record.map(
      lines => {
        val hoursub: String = lines.queryTime.substring(3,5) /*.mkString("\n")*/
       // println(hoursub)
        (hoursub.toInt, 1)
      }
    ))
      .reduceByKey((temp, item) => temp + item)   //按照小时分组,进行统计

    hourCount.foreachPartition(iter => iter.foreach(println))
    println("==================")

    hourCount.top(24)(Ordering.by(tuple=>tuple._2)).foreach(println)
    */

















  }

}
