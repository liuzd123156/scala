package com.ruozedata.spark.streaming.kafka

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
    带Receiver的Kakfa数据读取，local[n],n>1,需要单线程去用Receiver读取数据
 */
object Kafka0_8_ReceiverApp {
    def main(args: Array[String]): Unit = {
        val sc = new SparkConf().setMaster("local[2]")
                .setAppName("SparkSQLApp_functions")
        val ssc = new StreamingContext(sc,Seconds(10))

        /*  简单访问Kafka
        val zk = "hadoop01:2181,hadoop02:2181,hadoop03:2181/kafka"
        val group = "ruoze_group"
        val topics = Map(("test2",1))
        val kafkaStream = KafkaUtils.createStream(ssc,zk,group,topics)
        */

        val kafkaParams = Map[String,String](
            "group.id"->"ruoze_group",
            "zookeeper.connect"->"hadoop01:2181,hadoop02:2181,hadoop03:2181/kafka",
//            "auto.commit.enable"->"false",//不自动提交offset
            // 08版本好像不能用，没有010的异步提交实现directKafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
            "auto.offset.reset"->"smallest"
        )
        val topics = Map(("test2",1))
        val kafkaStream = KafkaUtils.createStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics,StorageLevel.MEMORY_AND_DISK_SER)

        kafkaStream.foreachRDD(rdd=>{
            rdd.take(10).foreach(println)
        })

        //返回示例数据：(19,ruoze19)
//        kafkaStream.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
