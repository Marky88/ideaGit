package cn.itcast.movie2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

/**
 * @author: Marky
 * @date: 2021/10/2 13:28
 * @description:
 */
object MovieDemo {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      .getOrCreate()

    import spark.implicits._

    val inputRDD: RDD[String] = spark.sparkContext.textFile("datas/input/ml-100k/u.dat")


    val ratingRDD: RDD[Row] = inputRDD.filter(line => null != line && line.trim.split("\t").length == 4)
      .mapPartitions { iter =>
        iter.map { line =>
          val Array(userId, itemId, rating, timestamp) = line.trim.split("\\s+")
          Row(userId, itemId, rating.toDouble, timestamp.toLong)
        }
      }


          val rowSchema: StructType = StructType(
                Array(
                  StructField("userId", StringType, nullable = true),
                  StructField("itemId", StringType, nullable = true),
                  StructField("rating", DoubleType, nullable = true),
                  StructField("timestamp", LongType, nullable = true)
                )
          )

    val ratingDF = spark.createDataFrame(ratingRDD,rowSchema)
    ratingDF.printSchema()
    ratingDF.show()

    spark.udf.register(
      "lower_name",
      (name:String) => name.toLowerCase
    )

    spark.sql(
      """
        |select name,lower_name(name) from demo;
        |""".stripMargin
    )

    spark.stop()
  }

}
