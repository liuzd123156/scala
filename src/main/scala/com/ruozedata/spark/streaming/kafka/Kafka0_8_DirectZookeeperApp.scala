package com.ruozedata.spark.streaming.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/*
    带Receiver的Kakfa数据读取，local[n],n>1,需要单线程去用Receiver读取数据
    Direct模式的可以为1，但是不建议
 */
object Kafka0_8_DirectZookeeperApp {
    def main(args: Array[String]): Unit = {
        val sc = new SparkConf().setMaster("local[2]")
                .setAppName("Kafka0_8_DirectZookeeperApp")
        val ssc = new StreamingContext(sc,Seconds(10))

        //加载存储offset的zk
        val retryPolicy = new ExponentialBackoffRetry(1000, 3)
        val client = CuratorFrameworkFactory.newClient("hadoop01:2181,hadoop02:2181,hadoop03:2181/kafka", retryPolicy)
        client.start()
        var status = ""
        while (status != "STARTED"){
            status = client.getState.toString
        }

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
            val map = obtainOffsets(group_id,x,client)
            if(map.size!=0){
                fromOffsets++=map
            }
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
            storeOffsets(group_id,offsetRanges,client)
        })

        //返回示例数据：(19,ruoze19)
//        kafkaStream.print()

        ssc.start()
        ssc.awaitTermination()
    }


    def obtainOffsets(groupName:String,topic:String, zookeeper:CuratorFramework): mutable.Map[TopicAndPartition, Long] = {
        val path = "/consumers/"+groupName+"/offsets/"+topic+"/partition"
        if(zookeeper.checkExists().forPath(path)==null){
            mutable.Map()
        }else{
            val maps = new mutable.HashMap[TopicAndPartition,Long]()
            val childs = zookeeper.getChildren.forPath(path)//应答结果是List<String>
            for(i <- 0 until childs.size()){
                val data = new String(zookeeper.getData().forPath(path+"/"+childs.get(i)))
                maps(TopicAndPartition(topic,childs.get(i).toInt)) = data.toLong
            }
            maps
        }
    }

    def storeOffsets(groupName:String,offsetsRanges:Array[OffsetRange],zookeeper:CuratorFramework) :Unit= {
        for(or <- offsetsRanges) {
            val path = "/consumers/" + groupName + "/offsets/" + or.topic + "/partition/" + or.partition
            if(zookeeper.checkExists().forPath(path)==null){
                zookeeper.create().creatingParentContainersIfNeeded().forPath(path)
            }
            zookeeper.setData().forPath(path,(or.untilOffset+"").getBytes())
        }
    }

    case class FromOffset(topicAndPartition: TopicAndPartition,fromOffset: Long)
}
