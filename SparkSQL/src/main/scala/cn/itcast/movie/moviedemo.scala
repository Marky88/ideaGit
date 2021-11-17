package cn.itcast.movie

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import scala.collection.immutable

/**
 * @author: Marky
 * @date: 2021/10/11 23:49
 * @description:
 */
object moviedemo {
  def main(args: Array[String]): Unit = {

    val sparkRDD: SparkSession = SparkSession.builder()
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .master("local[3]")
      .getOrCreate()

    import sparkRDD.implicits._

    val ratingRDD: Dataset[String] = sparkRDD.read.textFile("datas/input/ml-1m/ratings.dat")
    /*    println(s"Count= ${ratingRDD.count()}")
    println(ratingRDD.first())*/


    val mdf: DataFrame = ratingRDD
      .filter(lines => null != lines && lines.trim.split("::").length == 4)
      .map(line => {
        val arr: Array[String] = line.trim.split("::")
        //(user_id, item_id, ranking.toDouble, times.toLong)
        (arr(0), arr(1), arr(2), arr(3))
      }
      )
      .toDF("user_id", "item_id", "rating", "times")

   /* mdf.printSchema()
    mdf.show()

    println("================")*/

    //SQL
    mdf.createOrReplaceTempView("topMovie")

    val moviesql: DataFrame = sparkRDD.sql(
      """
        |
        |select
        |   item_id,count(*) as numb,round(avg(rating),2) as avgScore
        | from topMovie
        | group by item_id having numb >2000
        | order by avgScore desc,numb desc limit 10
        |
        |""".stripMargin
    )
/*    moviesql.printSchema()*/
    moviesql.show()

    moviesql.createOrReplaceTempView("tmpMovie")

    val frame = sparkRDD.sql(
      s"""
         |select
         |   cast( (sum(avgScore)) as Decimal(10,5)) as avg_decimal
         |
         |from
         |   tmpMovie
         |""".stripMargin
    )


    frame.show()

/*    moviesql.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .json("file:///E:\\Project\\Spark\\datas\\output\\top10demo2-movies\\")*/





    sparkRDD.stop()


  }
}
