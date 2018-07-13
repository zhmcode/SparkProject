package com.zhmcode.spark.jdbc

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhmcode on 2018/7/1 0001.
  */
object DeleteLocation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UpdateLocation").setMaster("local[2]")
    val sc = new SparkContext(conf)
    var conn: Connection = null
    var ps: PreparedStatement = null
    try {
      val sql = "delete from location_info where location = ?"
      conn = DriverManager.getConnection("jdbc:mysql://192.168.126.31:3306/sparkdatabase?useUnicode=true&characterEncoding=utf-8", "root", "Zhm@818919")
      ps = conn.prepareStatement(sql)
      ps.setString(1, "深圳")
      ps.execute()
    } catch {
      case e: Exception => println("myException")
    } finally {
      if (conn != null) {
        conn.close()
      }
      if (ps != null) {
        ps.close()
      }
    }
    sc.stop()
  }
}
