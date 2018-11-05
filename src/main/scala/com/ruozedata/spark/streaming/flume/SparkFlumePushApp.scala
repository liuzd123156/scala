package com.ruozedata.spark.streaming.flume

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.flume.FlumeUtils

object SparkFlumePushApp {
    def main(args: Array[String]): Unit = {
        val sc = new SparkConf().setMaster("local[2]")
                .setAppName("SparkSQLApp_functions")
        val ssc = new StreamingContext(sc,Seconds(10))
        //注意启动的ip是运行机器的ip
        val flumeStream = FlumeUtils.createStream(ssc, "192.168.3.84", 44444)
        flumeStream.map(x=>{
            new String( x.event.getBody.array())
        }).flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).print()
        ssc.start()
        ssc.awaitTermination()
    }
}
