package com.ruozedata.project01

import com.alibaba.fastjson.{JSON, JSONObject}
import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, Seconds, StreamingContext}
import scalikejdbc.config.DBs
import scalikejdbc.{NamedDB, SQL}
import org.influxdb.InfluxDB
import org.influxdb.InfluxDBFactory

import scala.collection.mutable
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

/*
    带Receiver的Kakfa数据读取，local[n],n>1,需要单线程去用Receiver读取数据
    Direct模式的可以为1，但是不建议
 */
object Kafka0_8_DirectJdbcApp {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession.builder()
                .master("local[2]")
                .appName("Kafka0_8_DirectJdbcApp")
                .getOrCreate()
        val ssc = new StreamingContext(spark.sparkContext,Seconds(5))

//        val slide_interval= new Duration(5 * 1000)
//        val window_length= new Duration(5 * 1000)

        var bcAlertList:Broadcast[java.util.List[String]] = null

        val influxDB = InfluxDBFactory.connect("http://hadoop01" + ":" + 8086, "admin", "admin")
        val rp = InfluxDBUtils.defaultRetentionPolicy(influxDB.version)
        val dbName = "ruoze_db"

        //加载存储offset的数据源
        DBs.setup('ruozedata)

        //指定消费者组名
        val group_id = "ruoze_group"
        //连接kafka的参数列表
        val kafkaParams = Map[String,String](
            "group.id"->group_id,
            "bootstrap.servers"->"hadoop01:9092,hadoop02:9092,hadoop03:9092",
            "auto.offset.reset"->"smallest"
        )
        //指定消费目标主题
        val topics = Set("g3_ruoze")

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


        //获取数据并转成对象，然后每5秒分割一个窗口，间隔也是5秒
//        kafkaStream.mapPartitions(x=>{//map mapPartitions也会把offset信息删除，所以不能用
//            val list = mutable.ListBuffer[CDHRoleLog] ()
//            while (x.hasNext){
//                val logs = x.next()._2
//                //判断数据是否符合初步规范
//                if(logs.contains("INFO") || logs.contains("WARN") || logs.contains("ERROR") || logs.contains("DEBUG")|| logs.contains("FATAL")){
//                    //解析数据并转成CDHRoleLog
//                    val cDHRoleLog:CDHRoleLog = {
//                        var json: JSONObject = null
//                        try{
//                            json = JSON.parseObject(logs)
//                        }catch {
//                            case e:Exception => e.printStackTrace()
//                        }
//                        if(json!=null){
//                            CDHRoleLog(json.getString("hostname"),json.getString("servicename"),json.getString("time"),json.getString("logtype"),json.getString("loginfo"))
//                        }else{
//                            null
//                        }
//                    }
//                    list+=cDHRoleLog
//                }
//            }
//            list.iterator
//        }).window(slide_interval,window_length)
        kafkaStream.foreachRDD(rdd=>{
            println(rdd.count())
            if(rdd.count()>0){

                val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges //RDD的offset
                bcAlertList = BroadcastUtil.getInstance.updateAndGet(spark, bcAlertList)
                val alertInfoList:java.util.List[String] = bcAlertList.value

                val cdhRdd = rdd.map(x=>{
                    var json: JSONObject = null
                    try{
                        json = JSON.parseObject(x._2)
                    }catch {
                        case e:Exception => e.printStackTrace()
                    }
                    if(json!=null){
                        CDHRoleLog(json.getString("hostname"),json.getString("servicename"),json.getString("time"),json.getString("logtype"),json.getString("loginfo"))
                    }else{
                        null
                    }
                }).filter(_!=null)

                val result = if(alertInfoList.size()>0){
                    cdhRdd.map(x=>{
                        (x.hostname+"_"+x.servicename+"_"+x.logtype,1)
                    }).reduceByKey(_+_)
                            .union(cdhRdd.map(x=>{
                                val alert:Int = {
                                    var i = 0
                                    for(alter_msg<-alertInfoList){
                                        if(x.loginfo.contains(alter_msg)){
                                            i = 1
                                        }
                                    }
                                    i
                                }
                                (x.hostname+"_"+x.servicename+"_alert",alert)
                            }).reduceByKey(_+_))
                }else{
                    cdhRdd.map(x=>{
                        (x.hostname+"_"+x.servicename+"_"+x.logtype,1)
                    }).reduceByKey(_+_)
                }

                val logtypecount = result.collect()

                var value = ""
                //
                for (rowlog <- logtypecount) {
                    val host_service_logtype = rowlog._1
                    value = value + "logtype_count,host_service_logtype=" + host_service_logtype + " count=" + String.valueOf(rowlog._2) + "\n"
                }

                if (value.length > 0) {
                    value = value.substring(0, value.length) //去除最后一个字符
                    //打印
                    System.out.println(value)
                    //保存
                    influxDB.write(dbName, rp, InfluxDB.ConsistencyLevel.ONE, value)
                }
                //同一批次的offset保存
                batchReplace(group_id,offsetRanges)

            }
        })


//        windowDStream.foreachRDD(rdd=>{
//            val logs = spark.createDataFrame(rdd,CDHRoleLog.getClass)
//            //该方式创建df，没有schema
//            println(logs.printSchema())
//            logs.createOrReplaceTempView("cdhrolelogs")
//
//            bcAlertList = BroadcastUtil.getInstance.updateAndGet(spark, bcAlertList)
//            val alertInfoList:java.util.List[String] = bcAlertList.value
//
//            var sqlstr:String = ""
//            if (alertInfoList.size > 0) { //定义alertsql
//                var alertsql = ""
//                println("print custom alert words:")
//                for (alertInfo <- alertInfoList) {
//                    println(alertInfo)
//                    alertsql = alertsql + " logInfo like '%" + alertInfo + "%' or"
//                }
//                alertsql = alertsql.substring(0, alertsql.length - 2)
//                //定义sql
//                val sqlstr = "SELECT hostname,servicename,logtype,COUNT(logtype) FROM cdhrolelogs GROUP BY hostname,servicename,logtype union all " + "SELECT t.hostname,t.servicename,t.logtype,COUNT(t.logtype) FROM " + "(SELECT hostname,servicename,'alert' logtype FROM cdhrolelogs where " + alertsql + ") t " + " GROUP BY t.hostname,t.servicename,t.logtype"
//            } else {
//                sqlstr = "SELECT hostname,servicename,logtype,COUNT(logtype) FROM cdhrolelogs GROUP BY hostname,servicename,logtype"
//            }
//
//            val logtypecount = spark.sql(sqlstr).collect()
//
//            var value = ""
//            //
//            for (rowlog <- logtypecount) {
//                val host_service_logtype = rowlog.get(0) + "_" + rowlog.get(1) + "_" + rowlog.get(2)
//                value = value + "logtype_count,host_service_logtype=" + host_service_logtype + " count=" + String.valueOf(rowlog.getLong(3)) + "\n"
//            }
//
//            if (value.length > 0) {
//                value = value.substring(0, value.length) //去除最后一个字符
//
//                //打印
//                System.out.println(value)
//                //保存
//                influxDB.write(dbName, rp, InfluxDB.ConsistencyLevel.ONE, value)
//            }
//        })


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
        println("save offset"+offsetRanges.size)
        NamedDB('ruozedata).localTx { implicit session =>
            for (offsetRange<- offsetRanges){
                SQL("replace into offset_storage(group_id,topic,partitions,offset) values(?,?,?,?)")
                        .bind(group_id,offsetRange.topic,offsetRange.partition,offsetRange.untilOffset)
                        .update().apply()
            }
        }
    }

    case class FromOffset(topicAndPartition: TopicAndPartition,fromOffset: Long)
    case class CDHRoleLog(hostname:String,servicename:String,linetimestamp:String,logtype:String,loginfo:String)
}
