package com.zhmcode.spark.basic

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhmcode on 2018/6/27 0027.
  * 根据日志统计出每个用户在站点所呆时间最长的前2个的信息
  */
object UserLocation {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("UserLocation")
    val sc = new SparkContext(conf)

    // 1. 先根据"手机号_站点"为唯一标识, 算一次进站出站的时间, 返回(手机号_站点, 时间间隔)
    val rdd1 = sc.textFile("D:\\sparkdata\\input\\bs_log").map(f = x => {
      val fields = x.split(",")
      val phone = fields(0)
      val eventType = fields(3)
      val time = fields(1)
      val location = fields(2)
      val mb = (phone, location)

      val timeLong = if (eventType == "1") -time.toLong else time.toLong
      (mb, time)
    })

    val rdd2 = rdd1.reduceByKey(_ + _)

    val rdd3 = sc.textFile("D:\\sparkdata\\input\\loc_info.txt").map(x => {
      val arr = x.split(",")
      val bs = arr(0)
      (bs, (arr(1), arr(2)))
    })

    val rdd4 = rdd2.map(t => (t._1._2, (t._1._1, t._2)))

    val rdd5 = rdd4.join(rdd3)
    val rdd6 = rdd2.map(t => (t._1._1, t._1._2, t._2)).groupBy(_._1).values.map(it => {
      it.toList.sortBy(_._3).reverse
    })
    println(rdd5.collect.toBuffer)


  }

}
