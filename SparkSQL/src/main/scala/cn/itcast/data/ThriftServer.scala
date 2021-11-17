package cn.itcast.data

import org.datanucleus.store.connection.ConnectionManager

import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}

object ThriftServer {

  def main(args: Array[String]): Unit = {
    var conn: Connection = null
    var pstmt: PreparedStatement = null
    var rs: ResultSet = null

      try {
      Class.forName("org.apache.hive.jdbc.HiveDriver")
      conn = DriverManager.getConnection(
        "jdbc:hive2://node1.itcast.cn:10000/db_hive",
        "root",
        "123456")

      val sql ="select * from db_hive.emp"
       pstmt = conn.prepareStatement(sql)
        rs = pstmt.executeQuery()

        while(rs.next()){
          println(s"empto = ${rs.getInt(1)}")
        }
    } catch {
      case e =>e.printStackTrace()
    } finally {
      if (pstmt != null){
        pstmt.close()
      }
    }
    if (conn != null){
      conn.close()
    }

  }

}
