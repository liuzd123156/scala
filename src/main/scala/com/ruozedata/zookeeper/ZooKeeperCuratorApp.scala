package com.ruozedata.zookeeper

import org.apache.curator.RetryPolicy
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry

import scala.collection.mutable.{ArrayBuffer, HashMap, Map}
import scala.util.Random


/*
    参考博客：https://www.jianshu.com/p/70151fc0ef5d
 */

object ZooKeeperCuratorApp {
    def main(args: Array[String]): Unit = {
        val retryPolicy = new ExponentialBackoffRetry(1000, 3)
        val client = CuratorFrameworkFactory.newClient("192.168.205.131:2181", retryPolicy)
        client.start()
        var status = ""
        while (status != "STARTED"){
            status = client.getState.toString
        }
        storeOffsets(getOffsetRanges,"G306",client)
        val result = obtainOffsets("ruoze_offset_topic","G306",client)
        for (map <- result){
            println("topic:"+map._1.topic+"     partition:"+map._1.partition+"      offset:"+map._2)
        }
        client.close()
    }

    def storeOffsets(offsetsRanges:ArrayBuffer[OffsetRange],groupName:String,zookeeper:CuratorFramework) :Unit= {
        for(or <- offsetsRanges) {
            val path = "/consumers/" + groupName + "/offsets/" + or.topic + "/partition/" + or.partition
            if(zookeeper.checkExists().forPath(path)==null){
                zookeeper.create().creatingParentContainersIfNeeded().forPath(path)
            }
            zookeeper.setData().forPath(path,(or.utilOffset+"").getBytes())
        }
    }

    def obtainOffsets(topic:String, groupName:String,zookeeper:CuratorFramework): Map[TopicAndPartition, Long] = {
        val path = "/consumers/"+groupName+"/offsets/"+topic+"/partition"
        if(zookeeper.checkExists().forPath(path)==null){
            Map()
        }else{
            val maps = new HashMap[TopicAndPartition,Long]()
            val childs = zookeeper.getChildren.forPath(path)//应答结果是List<String>
            for(i <- 0 until childs.size()){
                val data = new String(zookeeper.getData().forPath(path+"/"+childs.get(i)))
                maps(TopicAndPartition(topic,childs.get(i).toInt)) = data.toLong
            }
            maps
        }
    }

    /*
       随机生成偏移量记录对象
    */
    def getOffsetRanges():ArrayBuffer[OffsetRange]={
        val array = new ArrayBuffer[OffsetRange]()
        for(i<-0 to 2){
            array += OffsetRange("ruoze_offset_topic",getRandomInt(3),0,getRandomInt(1000).toLong)
        }
        array
    }

    def getRandomInt(n:Int): Int ={
        Random.nextInt(n)
    }
}


///*
//    每次消费数据的记录，包括消费的对象（对应主题和分区）和消费的数据偏移量
//    fromOffset:起始偏移量
//    utilOffset:终止偏移量
// */
//case class OffsetRange(val topic: String, val partition: Int,val fromOffset: Long,  val utilOffset: Long)
//
///*
//    主题对应的当前偏移量
// */
//case class TopicAndPartition(topic:String, partition:Int)