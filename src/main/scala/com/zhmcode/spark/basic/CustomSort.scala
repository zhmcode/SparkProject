package com.zhmcode.spark.basic

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhmcode on 2018/6/28 0028.
  * 自定义排序
  */
object CustomSort {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("CustomSort")
    val sc = new SparkContext(conf)
    val rdd1 = sc.parallelize(List(("yuihatan", 90, 28, 1), ("angelababy", 90, 27, 2), ("JuJingYi", 95, 22, 3)))
    val rdd2 = rdd1.sortBy(x => Girl(x._2, x._3), false)
    println(rdd2.collect.toBuffer)
  }
}

case class Girl( faceValue: Int, age: Int) extends Ordered[Girl] with Serializable {
  override def compare(that: Girl): Int = {
    if (this.faceValue == that.faceValue) {
      that.age - this.age
    } else {
      this.faceValue - that.faceValue
    }
  }
}