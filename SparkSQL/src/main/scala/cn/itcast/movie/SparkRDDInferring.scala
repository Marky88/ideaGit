package cn.itcast.movie

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object SparkRDDInferring {


  def main(args: Array[String]): Unit = {

    val sparkinfo: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      .getOrCreate()

    val inputRDD: RDD[String] = sparkinfo.sparkContext.textFile("datas/input/ml-100k/u.dat")

    val resultRow: RDD[Row] = inputRDD
      .filter(lines => null != lines && lines.trim.split("\\s+").length == 4)
      .map(line => {
        //  val Array(user_id, item_id, rating, timestamp) =
        val arr: Array[String] = line.trim.split("\\s+")
        Row(arr(0), arr(1), arr(2).toDouble, arr(3).toLong)
      }
      )

    val structspark: StructType = StructType(
      StructField("user_id", StringType, true) ::
        StructField("item_id", StringType, true) ::
        StructField("rating", DoubleType, true) ::
        StructField("timestamp", LongType, true) :: Nil
    )

    val sparkFrame: DataFrame = sparkinfo.createDataFrame(resultRow, structspark)

    sparkFrame.printSchema()
    sparkFrame.show()





  }



}
