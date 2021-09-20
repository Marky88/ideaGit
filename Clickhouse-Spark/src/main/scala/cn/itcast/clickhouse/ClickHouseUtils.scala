package cn.itcast.clickhouse

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseDataSource, ClickHouseStatement}
import org.apache.spark.sql.functions._

import java.sql.PreparedStatement


/**
 * @Description
 * @Author Marky
 * @Date 2021/7/9 20:06
 */
object ClickHouseUtils extends Serializable {
  val connection: ClickHouseConnection = null
  //连接clickhouse
  def createConnection(host:String,port:String,username:String,password:String): ClickHouseConnection ={
      Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
      val dataSource = new ClickHouseDataSource(s"jdbc:clickhouse://${host}:${port}")
      val connection: ClickHouseConnection = dataSource.getConnection(username, password)
      connection
  }


  //================  TODO： DML操作，封装方法 ===============================
  //插入数据：DataFrame到ClickHouse表
  def insertData(dataframe: DataFrame,
                 dbName: String, tableName: String): Unit = {

    val schema: StructType = dataframe.schema
    val columns: Array[String] = dataframe.columns
    val insertSql: String = createInsertSQL(dataframe, dbName, tableName)

    // TODO：针对每个分区数据进行操作，每个分区的数据进行批量插入
    dataframe.foreachPartition { iter =>

      var conn: ClickHouseConnection = null
      var pstmt: PreparedStatement = null
        try {
        conn = ClickHouseUtils.createConnection("node2.itcast.cn", "8123", "root", "123456")
        pstmt= conn.prepareStatement(insertSql)
        var counter:Int = 0
        //遍历每个分区中数据，将每条数据Row值进行设置
        iter.foreach{ row =>   //row: (areaName,category,id,money,timestamp)
          print("row的内容:" +row) //打印后:   row的内容:[北京,平板电脑,1,1450,2019-05-08T01:03.00Z]
          // 通过列名称来获取Row中下标的索引,再获取row中具体的值
          columns.foreach{column =>
            //通过列名称来获取Row中索引的下标
            val index: Int = schema.fieldIndex(column)
            //根据索引下标,从Row中获取值
            val value: Any = row.get(index)
            // 进行PreparedStatement设置值
            pstmt.setObject(index+1,value)
          }
          pstmt.setObject(columns.length+1,1)   //设置sign
          pstmt.setObject(columns.length+2,1)   //设置版本version
          //加入批次
          pstmt.addBatch()

          counter += 1
          //判断counter大小，如果大于1000 ，进行一次批量插入
          if(counter>1000){
            pstmt.executeBatch()
            counter = 0
          }
        }
        //执行最后,不能达到1000,批量插入 结束
        pstmt.executeBatch()
      } catch {
        case e => e.printStackTrace()
      } finally {
        // e. 关闭连接
        if(null != pstmt) pstmt.close()
        if(null != conn) conn.close()
      }

    }

  }


  //更新数据：依据主键，更新DataFrame数据到ClickHouse表
  def updateData(dataframe: DataFrame, dbName: String,
                 tableName: String, primaryField: String = "id"): Unit = {

    // 对DataFrame每个分区数据进行操作
    dataframe.foreachPartition(iter => {
      var conn: ClickHouseConnection = null
      var stmt: ClickHouseStatement = null

      try {
        conn = ClickHouseUtils.createConnection("node2.itcast.cn", "8123", "root", "123456")
        stmt = conn.createStatement()

        // TODO: 遍历分区中每条数据Row，构建更新Update语句，进行更新操作
        iter.foreach{ row =>
          val updateSql:String = createUpdateSQL(dbName,tableName,row,primaryField)

          stmt.executeUpdate(updateSql)
        }
      } catch {
        case e  => e.printStackTrace()
      } finally {
        if(null != stmt) stmt.close()
        if(null != conn) conn.close()
        }
      }
    )
  }

  def createUpdateSQL(dbName: String, tableName: String, row: Row,primaryField:String = "id"):String ={
    //update money='9999',timestamp='2021'
    row.schema.fields
      .map(filed =>filed.name)
      .filter(columnName => ! primaryField.equals(columnName))
      .map{columnName =>
        getFieldValue()  //需要自定义
      }


  val sql:String =
  s"""
  |alter table ${dbName}.${tableName}
  |update money='9999',timestamp='2021'
  |where ${primaryField}
  |""".stripMargin
    sql
  }


   // 删除数据：依据主键，将ClickHouse表中数据删除
  def deleteData(dataframe: DataFrame, dbName: String,
                 tableName: String, primaryField: String = "id"): Unit = {

  }


  /* ================  TODO： DDL操作，封装方法 =============================== */
  //执行sql语句
  def executeUpdate(sql: String): Unit = {
    var conn: ClickHouseConnection = null
    var pstmt: ClickHouseStatement = null
     try {
       conn = ClickHouseUtils.createConnection("node2.itcast.cn", "8123", "root", "123456")
       pstmt = conn.createStatement()

       pstmt.executeUpdate(sql)
     } catch {
       case  e =>e.printStackTrace()
     } finally {

       if (null != pstmt) pstmt.close()
       if(null != conn) conn.close()
     }
  }



  //todo 创建表 可以将创建表的String语句传入上面的executeUpdate(sql: String)进行执行
  def createTableDdl(dbName:String,tableName:String,dataFrame:DataFrame,primaryKeyFiled:String ="id"): String ={

    val  fieldStr: String = dataFrame.schema.fields.map { filed =>
      //获取字段名称
      val filedName: String = filed.name
      //获取字段类型
      val filedType: String = filed.dataType match {
        case StringType => "String"
        case IntegerType => "Int32"
        case FloatType => "Float32"
        case LongType => "Int64"
        case BooleanType => "UInt8"
        case DoubleType => "Float64"
        case DateType => "Date"
        case TimestampType => "DateTime"
        case x => throw new Exception(s"Unsupported type: ${x.toString}")
      }
      s"${filedName} ${filedType}"
    }.mkString(", \n")

    //创建表语句
    val createDdl:String =
      s"""
        |create table if not exists ${dbName}.${tableName}(
        |${fieldStr},
        |sign Int8,
        |version UInt8
        |)
        |ENGINE=VersionedCollapsingMergeTree(sign, version)
        |order by ${primaryKeyFiled}
        |""".stripMargin

    createDdl
  }


  //todo 删除表
  def dropTableDdl(dbName:String,tableName:String):String ={
    s"drop table ${dbName}.${tableName}"
  }


  /*
  INSERT INTO test.tbl_order (areaName, category, id, money, timestamp, sign, version)
  VALUES ('北京', '平板电脑', 1, '1450', '2019-05-08T01:03.00', 1, 1);
   */

  def createInsertSQL(dataFrame: DataFrame,dbName:String,tableName:String)={

    val columns: Array[String] = dataFrame.columns
    val fieldsStr: String = columns.mkString(",")
    val valuesStr:String = columns.map(_ =>"?").mkString(",")   //todo 使用占位符 方式SQL注入

    val resultStr :String =
      s"""
        |insert into ${dbName}.${tableName}
        |(${fieldsStr},sign,version)
        |values
        |(${valuesStr},?,?)
        |""".stripMargin
    resultStr
  }



}
