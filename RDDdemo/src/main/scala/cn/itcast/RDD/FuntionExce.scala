package cn.itcast.RDD

import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object FuntionExce {
  //log4j.rootLogger=INFO,R
  val sc : SparkContext={
    val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("parallize")
    SparkContext.getOrCreate(conf)

  }
  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
      val func = (index:Int,iter:Iterator[Int]) =>{
        iter.map(x=>"[partID:"+ index +",val: "+ x+ "]")
      }

    val rdd1: RDD[Int] = sc.parallelize(List(3, 6, 9, 2, 1,5),3)

    val indexIter: Array[String] = rdd1.mapPartitionsWithIndex(func).collect()
    indexIter.foreach(println)

/*    val rdd1: RDD[String] = sc.parallelize(List("dog", "hello", "lion", "cat"),2)
    val rdd2: RDD[(Int, String)] = rdd1.map(x => (x.length, x))
    val tuples: Array[(Int, String)] = rdd2.collect()
    tuples.foreach(println)
    println("===========")
    val ints: Array[Int] = rdd2.keys.collect()
    ints.foreach(println)
    println("===========")
    val collect: Array[String] = rdd2.values.collect
    collect.foreach(println)*/



/*    val rdd1: RDD[Int] = sc.parallelize(List(3, 6, 9, 2, 1))
    val rdd2: Array[Int] = rdd1.map(rd => rd * 2).collect
   // val ints: Array[Int] = rdd1.map(_ * 2).collect()
    rdd2.foreach(li =>println(li))*/

/*    val rdd1: RDD[String] = sc.parallelize(Array("a b c", "d e f", "h i j"))
    val wd: RDD[String] = rdd1.flatMap(_.split(" "))
    wd.foreach(li =>println(li))*/

/*    val rdd1: RDD[Int] = sc.parallelize(List(3, 6, 9, 2, 1))
    val rdd3: RDD[Int] = rdd1.filter(_ >= 5)
    rdd3.foreach(println)*/

/*    val rdd1: RDD[Int] = sc.parallelize(List(3, 6, 9))
    val rdd2: RDD[Int] = sc.parallelize(List(2, 1,3))
    val rdd3: RDD[Int] = rdd1.union(rdd2)
   // val collect: Array[Int] = rdd3.collect
   val collect: Array[Int] = rdd3.distinct.collect
  //  collect.foreach(println)

    val rdd4: RDD[Int] = rdd1.intersection(rdd2)
    val collect1: Array[Int] = rdd4.collect
   // println(collect1)
  //  collect1.foreach(rd =>println(rd))

    val rdd5: RDD[Int] = rdd1.subtract(rdd2)
    val ints: Array[Int] = rdd5.collect()
    ints.foreach(in => println(in))*/



  }

}
