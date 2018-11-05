package com.ruozedata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object WordCountApp {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf()
        val sc = new SparkContext(sparkConf)

        val lines = sc.textFile(args(0))
        val wc = lines.flatMap(_.split(",")).map((_,1)).reduceByKey(_+_)
        wc.collect().foreach(println)

        sc.stop()
    }
}
