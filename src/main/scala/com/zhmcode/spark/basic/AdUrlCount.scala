package com.zhmcode.spark.basic

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhmcode on 2018/6/27 0027.
  */
object AdUrlCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("UrlCount").setMaster("local[2]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.textFile("D:\\sparkdata\\input\\itcast.log").map(x=>{
      val arr = x.split("\t")
      (arr(1),1)
    })

    val rdd2 = rdd1.reduceByKey(_+_)

    val rdd3 = rdd2.map(x =>{
       val url = x._1
       val host = new URL(url).getHost
      (host,url,x._2)
    })

    val rdd4 = rdd3.groupBy(_._1).mapValues(t =>{
      t.toList.sortBy(_._3).reverse.take(3)
    })
    println(rdd4.collect().toBuffer)
    sc.stop()
  }
}
