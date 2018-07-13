package com.zhmcode.spark.sparksql

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhmcode on 2018/7/1 0001.
  */
object PersonSql2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PersonSql").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val line = sc.textFile("D:\\sparkdata\\input\\a.txt").map(_.split(" "))
    val personRdd = line.map(x => {
      Person2(x(0).toInt,x(1),x(2).toInt)
    })
    import sqlContext.implicits._
    val df = personRdd.toDF
    df.registerTempTable("t_person")
    sqlContext.sql("select * from t_person order by age desc limit 2").show
    sc.stop()
  }
}

case class Person2(id:Int,name:String,age:Int)
