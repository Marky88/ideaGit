package cn.itcast.movie

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql._
//采用反射的方式将RDD转换为DataFrame和Dataset
object RDDreverse {

  def main(args: Array[String]): Unit = {
    val sparkRDD: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      .getOrCreate()

     import sparkRDD.implicits._

    val inputDD: Dataset[String] = sparkRDD.read.textFile("datas/input/ml-100k/u.dat")
    val resultDF: Dataset[movierating] = inputDD
      .filter(lines => null != lines && lines.trim.split("\\s+").length == 4)
      .map(line => {
        val arr: Array[String] = line.trim.split("\\s+")
        movierating(arr(0), arr(1), arr(2).toDouble, arr(3).toLong)
      }
      )

    // 方法一： 通过隐式转换，直接将CaseClass类型RDD转换为DataFrame
    val ratingDF: DataFrame = resultDF.toDF()
    ratingDF.printSchema()
    ratingDF.show(15,truncate = false)

  sparkRDD.stop()


  }

}
