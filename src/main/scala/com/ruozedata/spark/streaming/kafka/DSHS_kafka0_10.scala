package com.ruozedata.spark.streaming.kafka


/**

import java.util.Date
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
  *
  * By ruozedata-J  20180826
  * kafka -> Version:0.10
  * 启动这个需要修改pom.xml为kafka0.10的
  * kafka自己维护记录offset
  *
  */

object DSHS_kafka0_10 {


  def main(args: Array[String]): Unit = {
    //定义变量
    val timeFormat = FastDateFormat.getInstance("yyyy/MM/dd HH:mm:ss.SSS")
    println("启动时间：" + timeFormat.format(new Date()))

    //流间隔 batch的时间
    var slide_interval = Seconds(1) //默认1秒
    if (args.length==1){
      slide_interval = Seconds(args(0).toLong) //自定义
    }

    val bootstrap_servers = "192.168.0.85:9092,192.168.0.86:9092,192.168.0.87:9092" //kakfa地址

    try {

      //1. Create context with 2 second batch interval

      val ss = SparkSession.builder()
        .appName("DSHS-0.1")
        .master("local[2]")
        .getOrCreate()
      val sc = ss.sparkContext
      val scc = new StreamingContext(sc, slide_interval)

      //2.设置kafka的map参数
      val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> bootstrap_servers
        , "key.deserializer" -> classOf[StringDeserializer]
        , "value.deserializer" -> classOf[StringDeserializer]
        , "group.id" -> "use_a_separate_group_id_for_each_stream"

        , "auto.offset.reset" -> "latest"
        , "enable.auto.commit" -> (false: java.lang.Boolean)

        , "max.partition.fetch.bytes" -> (2621440: java.lang.Integer) //default: 1048576
        , "request.timeout.ms" -> (90000: java.lang.Integer) //default: 60000
        , "session.timeout.ms" -> (60000: java.lang.Integer) //default: 30000
      )

      //3.创建要从kafka去读取的topic的集合对象
      val topics = Array("onlinelogs")

      //4.输入流
      val directKafkaStream = KafkaUtils.createDirectStream[String, String](
        scc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )

      //坑
      //? order window

      directKafkaStream.foreachRDD(

        rdd => {
          val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //RDD的offset

           //rdd-->DF -->sql
          rdd.foreachPartition(rows => {
            rows.foreach { row =>

                println(row.toString) //打印
                //TODO 业务处理


            }
          })


          //该RDD异步提交offset
          directKafkaStream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
        }
      )

      scc.start()
      scc.awaitTermination()
      scc.stop()
    } catch {
      case e: Exception =>
        println(e.getMessage)
    }
  }
}
*/