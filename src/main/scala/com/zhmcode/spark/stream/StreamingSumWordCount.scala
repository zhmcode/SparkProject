package com.zhmcode.spark.stream

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by zhmcode on 2018/6/28 0028.
  */
object StreamingSumWordCount {

  def main(args: Array[String]): Unit = {

    LoggerLevels.setStreamingLogLevels()

    val conf = new SparkConf().setAppName("StreamingSumWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //使用updateStateByKey必须设置checkpoint
    sc.setCheckpointDir("D:\\sparkdata\\output\\sparkck")
    val ssc = new StreamingContext(sc,Seconds(5))

    //接收数据,DStreaming是一个特殊的RDD
    val ds = ssc.socketTextStream("192.168.126.31",8888)
    val result = ds.flatMap(line=>{
      line.split(" ")
    }).map((_,1))

    val allResult = result.updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)
    allResult.print()

    ssc.start()
    ssc.awaitTermination()
  }

  //Seq 这个批次某个单词的次数
  //Option[Int]:以前的结果,上一次的结算结果，Opetion第一次的时候为空，getOrElse(0)
  //String key
  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.flatMap(it=>Some(it._2.sum + it._3.getOrElse(0)).map(x=>(it._1,x)))
    // iter.map(it=>(it._1,it._2.sum + it._3.getOrElse(0)))
    // 使用case必须使用{}
    iter.flatMap{case(x,y,z)=>Some(y.sum + z.getOrElse(0)).map(m=>(x, m))}
  }
}
