package com.zhmcode.spark.basic

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhmcode on 2018/6/27 0027.
  */
object WordCount02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(conf)

    val resultRdd = sc.textFile("D:\\sparkdata\\input\\a.txt").flatMap(_.split(" ")).map(x=>(x,1)).reduceByKey(_+_)
    println(resultRdd.collect().toBuffer)

  }
}
