package itcast.cn.jdbc;

/**
 * @Description
 * @Author Marky
 * @Date 2021/7/9 19:23
 */

import java.sql.*;

/**
 * 编写JDBC代码，从ClickHouse表查询分析数据
 * step1. 加载驱动类
 * step2. 获取连接Connection
 * step3. 创建PreparedStatement对象
 * step4. 查询数据
 * step5. 获取数据
 * step6. 关闭连接
 */
public class ClickHouseJDBCDemo {

    public static void main(String[] args) throws Exception {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet result  = null;
        try {
            //加载驱动
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            //获取连接
            conn = DriverManager.getConnection("jdbc:clickhouse://node2.itcast.cn:8123","root","123456");
            String sql = "select * from default.mt_table";
            //创建prepareStatement对象
            pstmt = conn.prepareStatement(sql);
            //执行查询
            result= pstmt.executeQuery();
            //获取数据
            while(result.next()){
                System.out.println(result.getString(2));
            }
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            //关闭连接
            if(result != null) result.close();
            if(pstmt != null) pstmt.close();
            if(conn != null) conn.close();
        }


    }


}
