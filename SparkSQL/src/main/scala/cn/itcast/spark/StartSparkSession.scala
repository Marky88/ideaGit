package cn.itcast.spark

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object StartSparkSession{

  //统计每个单词出现的次数
  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[2]")
      .getOrCreate()

      import spark.implicits._
    val inputDS: Dataset[String] = spark.read.textFile("datas/input/wordcount.txt")

/*    println(inputDS)
    println(s"count=${inputDS.count()}")
    println(s"first:${inputDS.first()}")*/

    inputDS.printSchema()
  //  inputDS.show(10,truncate=false)

    val resultDS: DataFrame = inputDS
      .select(explode(split(trim($"value"), "\\s+")).as("word"))
      .groupBy($"word").count()

    resultDS.printSchema()
    resultDS.show()




  }

}
