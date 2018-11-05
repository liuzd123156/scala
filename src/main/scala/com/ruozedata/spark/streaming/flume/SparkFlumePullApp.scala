package com.ruozedata.spark.streaming.flume

import org.apache.spark.SparkConf
import org.apache.spark.streaming.flume.FlumeUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkFlumePullApp {
    def main(args: Array[String]): Unit = {
        val sc = new SparkConf().setMaster("local[2]")
                .setAppName("SparkSQLApp_functions")
        val ssc = new StreamingContext(sc,Seconds(10))
        /** flume spark sink
        agent.sinks = spark
        agent.sinks.spark.type = org.apache.spark.streaming.flume.sink.SparkSink
        agent.sinks.spark.hostname = 192.168.205.131 #<hostname of the local machine>
        agent.sinks.spark.port = 44444 #<port to listen on for connection from Spark>
        agent.sinks.spark.channel = memoryChannel
          */
        val flumeStream = FlumeUtils.createPollingStream(ssc, "192.168.205.131", 44444)
        flumeStream.map(x=>{
            new String( x.event.getBody.array())
        }).flatMap(_.split(",")).map((_,1)).reduceByKey(_+_).print()
        ssc.start()
        ssc.awaitTermination()
    }
}
