package com.zhmcode.spark.stream

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zhmcode on 2018/6/28 0028.
  */
object StreamingWordCount {

  def main(args: Array[String]): Unit = {

    LoggerLevels.setStreamingLogLevels()

    val conf = new SparkConf().setAppName("StreamingSumWordCount").setMaster("local[2]")
    val sc = new SparkContext(conf)
    // sc.setCheckpointDir("D:\\sparkdata\\output\\sparkck")
    val ssc = new StreamingContext(sc,Seconds(5))

    //接收数据,DStreaming是一个特殊的RDD
    val ds = ssc.socketTextStream("192.168.126.31",8888)
    val result = ds.flatMap(line=>{
      line.split(" ")
    }).map((_,1)).reduceByKey(_+_)
    result.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
