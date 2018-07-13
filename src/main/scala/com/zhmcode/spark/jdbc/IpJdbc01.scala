package com.zhmcode.spark.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhmcode on 2018/6/28 0028.
  */
object IpJdbc01 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IpJdbc").setMaster("local[2]")
    val sc = new SparkContext(conf)
    var conn: Connection = null
    var ps: PreparedStatement = null
    val sql = "select * from location_info"
    try{
      conn = DriverManager.getConnection("jdbc:mysql://192.168.126.31:3306/sparkdatabase?useUnicode=true&characterEncoding=utf-8", "root", "Zhm@818919")
      ps = conn.prepareStatement(sql)
      val rs = ps.executeQuery()
      //将数据保存到ArrayBuffer中
     /* val rsmd = rs.getMetaData
      val size = rsmd.getColumnCount
      val buffer = new ArrayBuffer[scala.collection.mutable.HashMap[String, Any]]()
      while (rs.next()) {
        val map = mutable.HashMap[String, Any]()
        for (i <- 1 to size) {
          map += (rsmd.getColumnLabel(i) -> rs.getString(i))
        }
        buffer += map
      }
      println(buffer.toBuffer)*/
      //直接获取
      while(rs.next()){
        println(rs.getString("location")+"-------"+rs.getInt("counts"));
      }
    }catch {
      case e: Exception => println("myException")
    }finally {
      if (conn != null)
        conn.close()
      if (ps != null) {
        ps.close()
      }
    }
    sc.stop()
  }

}
