package com.zhmcode.spark.sparksql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * Created by zhmcode on 2018/7/1 0001.
  */
object SpecifyingSchema {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("PersonSql").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val personRDD = sc.textFile("D:\\sparkdata\\input\\a.txt").map(_.split(" "))
    //通过StructType直接指定每个字段的schema
    val schema = StructType{
      List(
        StructField("id", IntegerType, true),  //字段，类型，是否为空
        StructField("name",StringType,true),
        StructField("age",IntegerType,true)
      )
    }
    //将RDD映射到rowRDD
    val rowRDD = personRDD.map(p => Row(p(0).toInt, p(1).trim, p(2).toInt))
    //将schema信息应用到rowRDD上
    val personDataFrame = sqlContext.createDataFrame(rowRDD, schema)

    //注册表
    personDataFrame.registerTempTable("t_person")
    //执行SQL
    val df = sqlContext.sql("select * from t_person order by age desc limit 2")
    df.show()
    //停止Spark Context
    sc.stop()
  }
}
