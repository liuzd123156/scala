package com.ruozedata.spark.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
    带Receiver的Kakfa数据读取，local[n],n>1,需要单线程去用Receiver读取数据
 */
object Kafka0_8_DirectApp {
    def main(args: Array[String]): Unit = {
        val sc = new SparkConf().setMaster("local[2]")
                .setAppName("SparkSQLApp_functions")
        val ssc = new StreamingContext(sc,Seconds(10))


        val kafkaParams = Map[String,String](
            "group.id"->"ruoze_group_direct",
            "bootstrap.servers"->"hadoop01:9092,hadoop02:9092,hadoop03:9092",
            "auto.offset.reset"->"smallest"
        )
        val topics = Set("test2")
        val kafkaStream = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)

        kafkaStream.foreachRDD(rdd=>{
            //以下代码必须在DirectStream下才能使用
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //RDD的offset
            offsetRanges.foreach(x=>{
                print("partition:"+x.partition+"-")
                print("fromOffset:"+x.fromOffset+"-")
                print("untilOffset:"+x.untilOffset+"-")
                println("count:"+x.count())
            })
            rdd.take(10).foreach(println)
        })

        //返回示例数据：(19,ruoze19)
//        kafkaStream.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
