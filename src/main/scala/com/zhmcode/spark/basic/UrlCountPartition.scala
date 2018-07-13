package com.zhmcode.spark.basic

import java.net.URL

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

/**
  * Created by zhmcode on 2018/6/28 0028.
  */
object UrlCountPartition {

  def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("UrlCountPartition").setMaster("local[3]")
      val sc = new SparkContext(conf)

      val rdd1 = sc.textFile("D:\\sparkdata\\input\\itcast.log").map(x=>{
        val arr = x.split("\t")
        val url = arr(1)
        (url,1)
      })

      val rdd2 = rdd1.reduceByKey(_+_)

      val rdd3 = rdd2.map(x =>{
        val url = x._1
        val host = new URL(url).getHost
        //组成两个元素的元祖
        (host,(url,x._2))
      })

    // 获取不重复的host
     val arr = rdd3.map(_._1).distinct().collect()
     val partioner = new HostPartitioner(arr)


     val rdd4 = rdd3.partitionBy(partioner).mapPartitions(it =>{
       it.toList.sortBy(_._2._2).reverse.take(2).iterator
     })

    rdd4.saveAsTextFile("D:\\sparkdata\\output")
    sc.stop()
  }

}


class HostPartitioner(ins: Array[String]) extends Partitioner{
  var num =0
  val parMap = new mutable.HashMap[String,Int]()

  for(i<-ins){
    parMap.put(i,num)
    num+=1
  }

  override def numPartitions: Int = ins.length

  override def getPartition(key: Any): Int = {
    parMap.getOrElse(key.toString,0)
  }
}
