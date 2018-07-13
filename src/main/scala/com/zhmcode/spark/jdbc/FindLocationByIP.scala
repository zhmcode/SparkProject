package com.zhmcode.spark.jdbc

import java.io
import java.io.{BufferedReader, FileInputStream, InputStreamReader}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by zhmcode on 2018/6/28 0028.
  */
object FindLocationByIP {

  /**
    * ip 转换成整数
    */
  def ip2Long(ip: String) = {
    val framments = ip.split("\\.")
    var ipNum = 0L
    for (i <- 0 until framments.length) {
      ipNum = framments(i).toLong | ipNum << 8L
    }
    ipNum
  }

  /**
    * 读取本地文件到ArrayBuffer中
    */
  def readDatas(path: String): ArrayBuffer[String] = {
    val br = new BufferedReader(new InputStreamReader(new FileInputStream(path)))
    var flag = true
    val lines = new ArrayBuffer[String]()
    var s: String = null
    while (flag) {
      s = br.readLine()
      if (s != null) {
        lines += s
      } else {
        flag = false
      }
    }
    lines
  }

  def readData(path: String): ArrayBuffer[String] = {
    val br = new io.BufferedReader(new InputStreamReader(new FileInputStream(path)))
    var flag = true
    val lines = new ArrayBuffer[String]()
    var s: String = null
    while (flag) {
      s = br.readLine()
      if (s != null) {
        lines += s
      } else {
        flag = false
      }
    }
    lines
  }

  def binarySearch(lines : ArrayBuffer[String],ip:Long): Int ={
    var low = 0
    var high = lines.length - 1

    while (low <= high ){
      val middle = (low+high)/2
      if ((ip >= lines(middle).split("\\|")(2).toLong) && (ip <= lines(middle).split("\\|")(3).toLong))
        return middle
      if (ip < lines(middle).split("\\|")(2).toLong)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  def main(args: Array[String]): Unit = {
    val ip = "120.55.185.61"
    val ipNum = ip2Long(ip)
    val lines = readData("D:\\sparkdata\\input\\ip.txt")
    val index = binarySearch(lines, ipNum)
    print(lines(index))
  }


}
