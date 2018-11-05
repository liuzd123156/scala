package com.ruozedata.spark.streaming.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scalikejdbc.{NamedDB, SQL}
import scalikejdbc.config.DBs

import scala.collection.mutable

/*
    带Receiver的Kakfa数据读取，local[n],n>1,需要单线程去用Receiver读取数据
    Direct模式的可以为1，但是不建议
 */
object Kafka0_8_DirectJdbcApp {
    def main(args: Array[String]): Unit = {
        val sc = new SparkConf().setMaster("local[2]")
                .setAppName("Kafka0_8_DirectJdbcApp")
        val ssc = new StreamingContext(sc,Seconds(10))

        //加载存储offset的数据源
        DBs.setup('ruozedata)

        //指定消费者组名
        val group_id = "ruoze_group_direct"
        //连接kafka的参数列表
        val kafkaParams = Map[String,String](
            "group.id"->group_id,
            "bootstrap.servers"->"hadoop01:9092,hadoop02:9092,hadoop03:9092",
            "auto.offset.reset"->"smallest"
        )
        //指定消费目标主题
        val topics = Set("test2")

        //1.获取offset
        //创建空的offset列表
        val fromOffsets = mutable.Map[TopicAndPartition, Long]()
        //根据主题和组名获得所有offset列表
        topics.foreach(x=>{
            obtainOffsets(group_id,x).foreach(y=>{
                fromOffsets.put(y.topicAndPartition,y.fromOffset)
            })
        })
        //2.判断offset是否存在，如果存在则根据group/topic/offset获取数据，否则从头开始
        val kafkaStream = if(fromOffsets.size==0){
            KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](ssc,kafkaParams,topics)
        } else {
            val messageHandler = (mm:MessageAndMetadata[String,String]) => (mm.key(),mm.message())
            KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder,(String,String)](ssc,kafkaParams,fromOffsets.toMap,messageHandler)
        }
        //3.compute并获取offset，计算完成后保存offset
        kafkaStream.foreachRDD(rdd=>{
            //以下代码必须在DirectStream下才能使用
            val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //RDD的offset
//            offsetRanges.foreach(x=>{
//                print("partition:"+x.partition+"-")
//                print("fromOffset:"+x.fromOffset+"-")
//                print("untilOffset:"+x.untilOffset+"-")
//                println("count:"+x.count())
//            })
            rdd.take(10).foreach(println)
            batchReplace(group_id,offsetRanges)
        })

        //返回示例数据：(19,ruoze19)
//        kafkaStream.print()

        ssc.start()
        ssc.awaitTermination()
    }


    def obtainOffsets(group_id:String,topic:String):List[FromOffset] = {
        NamedDB('ruozedata).readOnly { implicit session =>
            SQL("select * from offset_storage where group_id=? and topic=?")
                    .bind(group_id,topic)
                    .map(rs => FromOffset(TopicAndPartition(rs.string("topic"),rs.int("partitions")),rs.long("offset")))
                    .list().apply()
        }
    }

    def batchReplace(group_id:String,offsetRanges:Array[OffsetRange]) :Unit= {
        NamedDB('ruozedata).localTx { implicit session =>
            for (offsetRange<- offsetRanges){
                SQL("replace into offset_storage(group_id,topic,partitions,offset) values(?,?,?,?)")
                        .bind(group_id,offsetRange.topic,offsetRange.partition,offsetRange.untilOffset)
                        .update().apply()
            }
        }
    }

    case class FromOffset(topicAndPartition: TopicAndPartition,fromOffset: Long)
}
