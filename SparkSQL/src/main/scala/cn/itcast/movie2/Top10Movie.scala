package cn.itcast.movie2

import org.apache.spark.sql.{DataFrame, Dataset, SaveMode, SparkSession}

import java.util.Properties

/**
 * @author: Marky
 * @date: 2021/10/2 14:41
 * @description:
 */
object Top10Movie {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[4]")
      .appName(this.getClass.getSimpleName.stripSuffix("$"))
      .config("spark.sql.shuffle.partitions","4")
      .getOrCreate()
    // 导入隐式转换
     import spark.implicits._


    val inputDS: Dataset[String] = spark.read.textFile("file:///E:\\Project\\Spark\\datas\\input\\ml-1m\\ratings.dat")
    val ratingDF: DataFrame = inputDS.filter(line => null != line && line.trim.split("::").length == 4)
      .map { movie =>
        val Array(userId, movieId, rating, timestamp) = movie.trim.split("::")
        (userId, movieId, rating.toDouble, timestamp.toLong)
      }.toDF("userId","movieId","rating","time")


  /*  ratingDF.printSchema()
    ratingDF.show(10)*/
    ratingDF.createOrReplaceTempView("movie_top10")

    ratingDF.printSchema()

    val movie_top10: DataFrame = spark.sql(
      """
        |select
        |movieId,round(avg(rating),2) AS avg_rating,count(movieId) as cnt_rating
        |from
        |movie_top10
        |group by movieId
        |having cnt_rating >2000
        |order by  avg_rating DESC,cnt_rating DESC
        |limit 10
        |""".stripMargin
    )

    //movie_top10.show()

    //保存
    //至数据库
    movie_top10.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("driver","com.mysql.cj.jdbc.Driver")
      .option("user","root")
      .option("password","123456")
      .jdbc("jdbc:mysql://node1.vlion.cn:3306/?serverTimezone=UTC&characterEncoding=utf8&useUnicode=true",
            "db_test.tb_top10_movie",
        new Properties)


  // 至外部文件
    movie_top10.write
      .mode(SaveMode.Overwrite)
      .format("json")
      .save("file:///E:\\Project\\Spark\\datas\\output\\top10-movies\\")

    movie_top10.write
      .mode(SaveMode.Overwrite)
      .csv("file:///E:/Project/Spark/")

    movie_top10.unpersist()




    val githubDS: Dataset[String] = spark.read.textFile("datas/json/2015-03-01-11.json.gz")
    import org.apache.spark.sql.functions._
    githubDS.select(
      get_json_object($"value","$.id").as("id"),
      get_json_object($"value","$.type").as("type")

    )

    spark.stop()
  }

}
