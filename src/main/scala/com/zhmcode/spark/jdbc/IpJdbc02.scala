package com.zhmcode.spark.jdbc

import java.sql.{Connection, Date, DriverManager, PreparedStatement}

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhmcode on 2018/6/28 0028.
  */
object IpJdbc02 {
  def ip2Long(ip: String): Long = {
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length) {
      ipNum = fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  def binarySearch(lines: Array[(String, String, String)], ip: Long): Int = {
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1.toLong) && (ip <= lines(middle)._2.toLong))
        return middle
      if (ip < lines(middle)._1.toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IpJdbc2").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val rdd1 = sc.textFile("D:\\sparkdata\\input\\ip.txt").map(line =>{
      val fields = line.split("\\|")
      val start_num = fields(2)
      val end_num = fields(3)
      val province = fields(6)
      (start_num,end_num,province)
    })

    val rpRulesBroadcast = rdd1.collect()
    val ipRulesBroadcast = sc.broadcast(rpRulesBroadcast)

    val rdd3 = sc.textFile("D:\\textdata\\ip.txt").map(line =>{
      val fields = line.split("\\|")
      fields(1)
    })

    val result = rdd3.map(ip =>{
      val ipNum = ip2Long(ip.toString)
      val index = binarySearch(ipRulesBroadcast.value,ipNum)
      val info = ipRulesBroadcast.value(index)
      //(ip的起始Num， ip的结束Num，省份名)
      info
    }).map(t => (t._3, 1)).reduceByKey(_+_)

    result.foreachPartition(data2MySQL(_))

  }


  def data2MySQL(iterator: Iterator[(String, Int)]): Unit ={
    var conn: Connection = null;
    var ps: PreparedStatement = null;
    val  sql = "INSERT INTO location_info (location, counts, accesse_date) VALUES (?, ?, ?)"
    try{
      conn = DriverManager.getConnection("jdbc:mysql://192.168.126.31:3306/sparkdatabase?useUnicode=true&characterEncoding=utf-8","root","Zhm@818919")
      iterator.foreach(line => {
        ps = conn.prepareStatement(sql)
        ps.setString(1,  line._1)
        ps.setInt(2, line._2)
        ps.setString(3, new Date(System.currentTimeMillis()).toString)
        ps.executeUpdate()
      })
    }catch {
      case e: Exception => println("myException")
    }finally {
      if (ps != null)
        ps.close()
      if (conn != null)
        conn.close()
    }

  }

}
