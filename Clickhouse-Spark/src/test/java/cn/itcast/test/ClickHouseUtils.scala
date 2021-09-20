package cn.itcast.test

import java.sql.{PreparedStatement, Statement}

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}
import ru.yandex.clickhouse.{ClickHouseConnection, ClickHouseDataSource, ClickHouseStatement}

/**
 * 工具类：实现在ClickHouse数据库中创建表、删除表以及对数据RUD操作，定义方法
 */
object ClickHouseUtils {
	
	/**
	 * 创建ClickHouse的连接实例，返回连接对象，通过连接池获取连接对象
	 *
	 * @param host ClickHouse 服务主机地址
	 * @param port ClickHouse 服务端口号
	 * @param username 用户名称，默认用户：default
	 * @param password 密码，默认为空
	 * @return Connection 对象
	 */
	def createConnection(host: String, port: String,
	                     username: String, password: String): ClickHouseConnection = {
		// 加载驱动类
		Class.forName("ru.yandex.clickhouse.ClickHouseDriver")
		// TODO: 使用ClickHouse中提供ClickHouseDataSource获取连接对象
		val datasource: ClickHouseDataSource = new ClickHouseDataSource(s"jdbc:clickhouse://${host}:${port}")
		// 获取连接对象
		val connection: ClickHouseConnection = datasource.getConnection(username, password)
		// 返回对象
		connection
	}
	
	/**
	 * 传递Connection对象和SQL语句对ClickHouse发送请求，执行更新操作
	 *
	 * @param sql 执行SQL语句或者DDL语句
	 */
	def executeUpdate(sql: String): Unit = {
		// 声明变量
		var conn: ClickHouseConnection = null
		var pstmt: ClickHouseStatement = null
		try{
			// a. 获取ClickHouse连接对象
			conn = ClickHouseUtils.createConnection("node2.itcast.cn", "8123", "root", "123456")
			// b. 获取PreparedStatement实例对象
			pstmt = conn.createStatement()
			// c. 执行更新操作
			pstmt.executeUpdate(sql)
		}catch {
			case e: Exception => e.printStackTrace()
		}finally {
			// 关闭连接
			if (null != pstmt) pstmt.close()
			if (null != conn) conn.close()
		}
	}
	
	/**
	 * 依据DataFrame数据集中约束Schema信息，构建ClickHouse表创建DDL语句
	 *
	 * @param dbName 数据库的名称
	 * @param tableName 表的名称
	 * @param schema DataFrame数据集约束Schema
	 * @param primaryKeyField ClickHouse表主键字段名称
	 * @return ClickHouse表创建DDL语句
	 */
	def createTableDdl(dbName: String, tableName: String,
	                   schema: StructType, primaryKeyField: String = "id"): String = {
		/*
		   1. 构建字段语句
			    areaName String,
				category String,
				id Int64,
				money String,
				timestamp String
		 */
		val fieldStr: String = schema.fields.map{field =>
			// 获取列名称
			val name: String = field.name
			// 获取数据类型
			val dataType: String = field.dataType match {
				case DataTypes.StringType => "String"
				case DataTypes.LongType => "Int64"
				case _ => "String"
			}
			// 拼凑字符串
			s"${name} ${dataType}"
		}.mkString(",\n")

		// 2. 创建表DDL语句
		s"""
		   |CREATE TABLE IF NOT EXISTS ${dbName}.${tableName} (
		   |${fieldStr},
		   |sign Int8,
		   |version UInt8
		   |)
		   |ENGINE=VersionedCollapsingMergeTree(sign, version)
		   |ORDER BY ${primaryKeyField}
		   |""".stripMargin
	}
	
	/**
	 * 依据数据库名称和表名称，构建ClickHouse中删除表DDL语句
	 *
	 * @param dbName 数据库名称
	 * @param tableName 表名称
	 * @return 删除表的DDL语句
	 */
	def dropTableDdl(dbName: String, tableName: String): String = {
		s"DROP TABLE IF EXISTS ${dbName}.${tableName}"
	}
	
	/**
	 * 构建数据插入ClickHouse中SQL语句
	 *
	 * @param dbName 数据库名称
	 * @param tableName 表的名称
	 * @param columns 列名称
	 * @return INSERT 插入语句
	 */
	def createInsertSQL(dbName: String, tableName: String, columns: Array[String] ): String = {
		// 1. 拼凑列名称字符串 -> areaName, category, id, money, timestamp
		val columnStr: String = columns.mkString(", ")
		
		// 2. 拼凑每列对应值的字符串 -> '北京', '平板电脑', 1, '1450', '2019-05-08T01:03.00'  TODO: 此处使用占位符？
		val valueStr: String = columns.map(_ => "?").mkString(", ")
		
		// 3. 插入INSERT语句
		s"INSERT INTO ${dbName}.${tableName} (${columnStr}, sign, version) VALUES (${valueStr}, ?, ?)"
	}
	
	/**
	 * 插入数据：DataFrame到ClickHouse表
	 */
	def insertData(dataframe: DataFrame,
	               dbName: String, tableName: String): Unit = {
		val columns: Array[String] = dataframe.columns
		// 获取插入INSERT 语句
		val insertSQL: String = createInsertSQL(dbName, tableName, columns)
		
		// 对DataFrame分区进行操作，将数据插入到ClickHouse表中：1. 分区操作，转换Row为INSERT语句；2. 设置具体的值
		dataframe.foreachPartition{iter =>
			// 定义变量
			var conn: ClickHouseConnection = null
			var pstmt: PreparedStatement = null
			
			try{
				// a. 获取连接
				conn = createConnection("node2.itcast.cn", "8123", "root", "123456")
				// b. 获取Statement对象
				pstmt = conn.prepareStatement(insertSQL)
				
				// TODO: c. 遍历分区中每条数据，获取Row值，设置占位符对应值
				var counter = 0
				iter.foreach{row =>
					/*
					(areaName, category, id, money, timestamp) VALUES (?, ?, ?, ?, ?)
											|
											|row
											|
					 */
					// 依据列名称获取对应的值： 1. 列名称获取索引；2. 依据索引获取值
					columns.foreach{columnName =>
						// 列名称获取索引
						val columnIndex: Int = row.fieldIndex(columnName)
						// 依据索引获取值
						val columnValue = row.get(columnIndex)
						// TODO: 设置每列具体的值
						pstmt.setObject(columnIndex + 1, columnValue)
					}
					// 设置sign和version值
					pstmt.setObject(columns.length + 1, 1)
					pstmt.setObject(columns.length + 2, 1)
					
					// 加入批次中
					pstmt.addBatch()
					counter += 1
					// TODO： 判断批次中数据量，如果大于1000条件，进行批量插入一次
					if(counter >= 1000){
						pstmt.executeBatch()
						counter = 0
					}
				}
				
				// d. 批量插入
				if(counter > 0) {
					pstmt.executeBatch()
				}
			}catch {
				case e: Exception => e.printStackTrace()
			}finally {
				// e. 关闭连接
				if(null != conn) conn.close()
				if(null != pstmt) pstmt.close()
			}
		}
	}
	
	/**
	 * 依据列名称从Row中获取值
	 */
	def getColumnValue(row: Row, columnName: String): Any = {
		// 获取索引
		val columnIndex: Int = row.fieldIndex(columnName)
		// 获取值
		row.get(columnIndex)
	}
	
	/**
	 * 将Row数据，转换为相应ALTER UPDATE语句
	 */
	def createUpdateSQL(dbName: String, tableName: String,
	                    row: Row, primaryField: String = "id"): String = {
		// 获取主键值
		val primaryValue: Any = getColumnValue(row, primaryField)
	
		// 更新数据时，设置的字段与值 -> money = '9999', timestamp = '2020-12-08T01:03.00Z'
		val updateStr: String = row.schema
			// 获取更新数据集中所有列名称
			.fieldNames
			// 过滤掉主键字段
    		.filter(columnName => ! primaryField.equals(columnName))
			// 遍历列名称，获取值，进行组合字段
    		.map{columnName =>
			    val columnValue = getColumnValue(row, columnName)
			    // 拼凑字符串
			    s"${columnName} = '${columnValue}'"
		    }
			// 转换为字符串，使用逗号隔开
    		.mkString(", ")
		
		// 依据Row每条数据，构建ALTER UPDATE 语句
		s"ALTER TABLE ${dbName}.${tableName} UPDATE ${updateStr} WHERE ${primaryField} = ${primaryValue}"
	}
	
	/**
	 * 更新数据：依据主键，更新DataFrame数据到ClickHouse表
	 */
	def updateData(dataframe: DataFrame, dbName: String,
	               tableName: String, primaryField: String = "id"): Unit = {
		// 对DataFrame分区进行操作，将数据更新到ClickHouse表中：1. 分区操作，转换Row为LATER UPDATE语句；2. 设置具体的值
		dataframe.foreachPartition{iter =>
			// 定义变量
			var conn: ClickHouseConnection = null
			var pstmt: Statement = null
			
			try{
				// a. 获取连接
				conn = createConnection("node2.itcast.cn", "8123", "root", "123456")
				// b. 获取Statement对象
				pstmt = conn.createStatement()
				
				// TODO: c. 遍历分区中每条数据，获取Row值，转换为ALTER UPDATE 语句
				/*
				ALTER TABLE  test.tbl_order
					UPDATE money = '9999', timestamp = '2020-12-08T01:03.00Z' WHERE id = 3 ;
				 */
				iter.foreach{row =>
					val updateSQL: String = createUpdateSQL("test", "tbl_order", row)
					println(updateSQL)
					// d. 执行更新操作
					executeUpdate(updateSQL)
				}
			}catch {
				case e: Exception => e.printStackTrace()
			}finally {
				// e. 关闭连接
				if(null != conn) conn.close()
				if(null != pstmt) pstmt.close()
			}
		}
		
	}
	
	/**
	 * 删除数据：依据主键，将ClickHouse表中数据删除
	 */
	def deleteData(dataframe: DataFrame, dbName: String,
	               tableName: String, primaryField: String = "id"): Unit = {
		// 对DataFrame分区进行操作，依据主键删除ClickHouse表中数据
		dataframe.foreachPartition{iter =>
			// 定义变量
			var conn: ClickHouseConnection = null
			var pstmt: Statement = null
			
			try{
				// a. 获取连接
				conn = createConnection("node2.itcast.cn", "8123", "root", "123456")
				// b. 获取Statement对象
				pstmt = conn.createStatement()
				
				// TODO: c. 遍历分区中每条数据，获取Row值，转换为ALTER UPDATE 语句
				/*
					ALTER TABLE test.tbl_order DELETE WHERE id = "3" ;
				 */
				iter.foreach{row =>
					// 依据主键列名称获取对应的值
					val primaryValue = getColumnValue(row, primaryField)
					val deleteSQL: String = s"ALTER TABLE ${dbName}.${tableName} DELETE WHERE ${primaryField} = ${primaryValue}"
					println(deleteSQL)
					// d. 执行更新操作
					executeUpdate(deleteSQL)
				}
			}catch {
				case e: Exception => e.printStackTrace()
			}finally {
				// e. 关闭连接
				if(null != conn) conn.close()
				if(null != pstmt) pstmt.close()
			}
		}
	}
	
}
