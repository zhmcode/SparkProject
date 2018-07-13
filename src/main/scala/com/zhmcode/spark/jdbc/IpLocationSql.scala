package com.zhmcode.spark.jdbc

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhmcode on 2018/6/28 0028.
  */
object IpLocationSql {
  val conf = new SparkConf().setMaster("local[2]").setAppName("IpLocationSql")
  val sc = new SparkContext(conf)

  val rdd1 = sc.textFile("D:\\sparkdata\\input\\ip.txt").map(line => {
    val fields = line.split("")
    val start_num = fields(2)
    val end_num = fields(3)
    val provicnce = fields(6)
    (start_num,end_num,provicnce)
  })

  val ipRulesArray = rdd1.collect()
  val ipRulesBroadcast = sc.broadcast(ipRulesArray)

  //加载要处理的数据
  val ipsRDD = sc.textFile("D:\\sparkdata\\input\\access_log").map(line => {
    val fields = line.split("\\|")
    fields(1)
  })


}
