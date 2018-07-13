package com.zhmcode.spark.stream

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
  * Created by zhmcode on 2018/7/10 0010.
  */
object KafkaWordCount {

  def main(args: Array[String]): Unit = {
    LoggerLevels.setStreamingLogLevels()
    val conf = new SparkConf().setAppName("KafkaWordCount").setMaster("local[3]")
    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc,Seconds(5))

    ssc.checkpoint("D:\\sparkdata\\ck\\streaming")
    val Array(zkQuorum, group, topics, numThreads) = args
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val data = KafkaUtils.createStream(ssc,zkQuorum,group,topicMap,StorageLevel.MEMORY_AND_DISK_SER)

    val words = data.map(_._2).flatMap(_.split(" "))
    val wordCounts = words.map((_, 1)).updateStateByKey(updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)

    ssc.start()
    ssc.awaitTermination()
  }

  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
     iter.map(it=>(it._1,it._2.sum + it._3.getOrElse(0)) )
  }

}
