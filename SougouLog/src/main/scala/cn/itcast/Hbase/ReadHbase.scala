package cn.itcast.Hbase


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase
import org.apache.hadoop.hbase.{Cell, CellUtil, HBaseConfiguration}
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReadHbase {

  // 设置连接Zookeeper属性
  val conf: Configuration = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.quorum", "node1.itcast.cn")
  conf.set("hbase.zookeeper.property.clientPort", "2181")
  conf.set("zookeeper.znode.parent", "/hbase")
  // 设置将数据保存的HBase表的名称
  // conf.set(TableOutputFormat.OUTPUT_TABLE, "htb_wordcount")
  conf.set(TableInputFormat.INPUT_TABLE,"htb_wordcount")

  val sc:SparkContext = {
    val sparkconf: SparkConf = new SparkConf()
                .setAppName(this.getClass.getSimpleName.stripSuffix("$"))
                .setMaster("local[2]")
      // TODO: 设置使用Kryo 序列化方式  由于[ImmutableBytesWritable,Result]不支持java序列化,而Spark支持java序列化,
    //  todo 所有需要对输入的[ImmutableBytesWritable,Result]类型进行转换 ,设置Spark Application使用Kryo序列
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // TODO: 注册序列化的数据类型
      .registerKryoClasses(Array(classOf[ImmutableBytesWritable], classOf[Result]))
      SparkContext.getOrCreate(sparkconf)
  }

  //从hbase读数据到spark
  def main(args: Array[String]): Unit = {

    val hbaseRDD: RDD[(ImmutableBytesWritable, Result)] = sc.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    )
    println(hbaseRDD.count())

    hbaseRDD.take(5).foreach{case (rowkey,result) =>
      val cells: Array[Cell] = result.rawCells()   //得到一个cell数组,一个cell包含一列的数据:rowkey,列族,value,时间戳
      cells.foreach(cell =>
        println(s"rowkey:${Bytes.toString(result.getRow)} + " +
          s"Family: ${Bytes.toString(CellUtil.cloneFamily(cell))} +" +
          s" qualifier:${Bytes.toString(CellUtil.cloneQualifier(cell))}"+
          s"value: ${Bytes.toString(CellUtil.cloneValue(cell))}"
        )
      )
    }


    Thread.sleep(100000)
    sc.stop()

  }

}
