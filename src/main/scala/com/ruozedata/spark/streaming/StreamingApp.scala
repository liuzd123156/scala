package com.ruozedata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object StreamingApp {
    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
        val ssc = new StreamingContext(conf, Seconds(1))

        val lines = ssc.socketTextStream("192.168.205.131", 9999)

        ssc.start()
        ssc.awaitTermination()
    }
}
