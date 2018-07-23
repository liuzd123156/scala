package com.ruozedata

import org.apache.spark.{SparkConf, SparkContext}

object SparkContextApp {
  var s:String = _
  def main(args: Array[String]): Unit = {
    println(s)
//    val sparkConf = new SparkConf().setAppName("").setMaster("local[2]")
//    val sc = new SparkContext(sparkConf)
//    sc.stop()
  }
}
