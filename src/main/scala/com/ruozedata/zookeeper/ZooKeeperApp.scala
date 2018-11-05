package com.ruozedata.zookeeper

import org.apache.zookeeper.Watcher.Event
import org.apache.zookeeper._

import scala.util.Random
import java.util.concurrent.CountDownLatch

import scala.collection.mutable._


//import org.apache.zookeeper.data.Stat
/**
  * /consumers/G301/offsets/ruoze_offset_topic/partition/0
  * /consumers/G301/offsets/ruoze_offset_topic/partition/1
  * /consumers/G301/offsets/ruoze_offset_topic/partition/2
  */

object ZooKeeperApp {

    //等待zk创建成功，配合Watcher里的process方法
    val connected = new CountDownLatch(1)
    val zookeeper = new ZooKeeper("192.168.205.131:2181",3000,new Watcher {
        override def process(event: WatchedEvent): Unit = {
            println("create zookeeper client begin...")
            if(Event.KeeperState.SyncConnected == event.getState){
                println("create zookeeper client end...")
                connected.countDown()
            }
        }
    })

    def main(args: Array[String]): Unit = {
        connected.await()
        storeOffsets(getOffsetRanges,"G306")
        val result = obtainOffsets("ruoze_offset_topic","G306")
        for (map <- result){
            println("topic:"+map._1.topic+"     partition:"+map._1.partition+"      offset:"+map._2)
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

    /*
        存储偏移量
     */
    def storeOffsets(offsetsRanges:ArrayBuffer[OffsetRange],groupName:String) :Unit= {
        for(or <- offsetsRanges){
            val path = "/consumers/"+groupName+"/offsets/"+or.topic+"/partition/"+or.partition
            createOrExistsPath(path,zookeeper)
            zookeeper.setData(path,(or.utilOffset+"").getBytes(),-1)
        }
    }

    /*
        根据topic和groupName查询所属分区和对应偏移量
     */
    def obtainOffsets(topic:String, groupName:String): Map[TopicAndPartition, Long] = {
        val path = "/consumers/"+groupName+"/offsets/"+topic+"/partition"
        if(zookeeper.exists(path,false)==null){
            Map(TopicAndPartition(topic,-1) -> -1)
        } else {
            val childs = zookeeper.getChildren(path,false)
            val maps = new HashMap[TopicAndPartition,Long]()
            for(i<-0 until childs.size()){
                val partition = childs.get(i)
                val partition_path = path+"/"+partition
                val result = new String(zookeeper.getData(partition_path,false, null))
                maps(TopicAndPartition(topic,partition.toInt)) = result.toLong
            }
            maps
        }
    }

    /*
        检查znode是否存在，如果不存在就创建，创建成功后返回创建路径
     */
    def createOrExistsPath(path:String,zooKeeper: ZooKeeper): String ={
        println("check "+path)
        val stat = zookeeper.exists(path,false)
        if(stat!=null){
            path
        }else{
            if(path.lastIndexOf("/")!=0){
                val paths = path.substring(0,path.lastIndexOf("/"))
                createOrExistsPath(paths,zooKeeper)
            }
            zookeeper.create(path,"".getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT)
        }
    }

}


/*
    每次消费数据的记录，包括消费的对象（对应主题和分区）和消费的数据偏移量
    fromOffset:起始偏移量
    utilOffset:终止偏移量
 */
case class OffsetRange(val topic: String, val partition: Int,val fromOffset: Long,  val utilOffset: Long)

/*
    主题对应的当前偏移量
 */
case class TopicAndPartition(topic:String, partition:Int)