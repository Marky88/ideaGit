package cn.itcast.Hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object WriteHbase {

  val conf: Configuration = HBaseConfiguration.create()
  // 设置连接Zookeeper属性
  conf.set("hbase.zookeeper.quorum", "node1.itcast.cn")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("zookeeper.znode.parent", "/hbase")
  // 设置将数据保存的HBase表的名称
 // conf.set(TableOutputFormat.OUTPUT_TABLE, "htb_wordcount")
  conf.set(TableOutputFormat.OUTPUT_TABLE,"htb_wordcount")


  val sc: SparkContext =
                        {
                          val sparkConf = new SparkConf()
                            .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
                            .setMaster("local[2]")
                          SparkContext.getOrCreate(sparkConf)
                        }
  //向Hbase中写入数据
  def main(args: Array[String]): Unit = {
    val inputRDD: RDD[String] = sc.textFile("datas/input/wordcount.txt")
    val resultRDD: RDD[(String, Int)] = inputRDD
      .filter(line => (null != line) && line.trim.length > 0)
      .flatMap(lines => lines.split("\\s+"))
      .map(word => (word, 1))
      .reduceByKey((temp, item) => temp + item)

     // resultRDD.foreach(println)


    val putsRDD: RDD[(ImmutableBytesWritable, Put)] = resultRDD.map{case (word, count) =>
      // 其一、构建RowKey对象
      val rowKey: ImmutableBytesWritable = new ImmutableBytesWritable(Bytes.toBytes(word))
      // 其二、构建Put对象
      val put: Put = new Put(rowKey.get())
      // 设置字段的值
      put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("count"), Bytes.toBytes(count.toString))

      // 其三、返回二元组(RowKey, Put)
      rowKey -> put
    }



    putsRDD.saveAsNewAPIHadoopFile(
      "datas/hbase/htb_wordcount/", //
      classOf[ImmutableBytesWritable], //
      classOf[Put], //
      classOf[TableOutputFormat[ImmutableBytesWritable]], //
      conf
    )

    Thread.sleep(100000)

    sc.stop()


  }

}
