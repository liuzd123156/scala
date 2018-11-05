package com.ruozedata.project01

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import scalikejdbc.config.DBs
import scalikejdbc.{NamedDB, SQL}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

object SparkHiveLogAnalysisApp {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
                .builder()
//                .master("local[2]")
                .enableHiveSupport()
                .appName("SparkHiveLogAnalysisApp")
                .getOrCreate()

        DBs.setup('loganalysis)
        var bcAlertList:Broadcast[java.util.List[String]] = null
        val conf = spark.sparkContext.getConf
        val day = conf.get("spark.app.day")
        val hiveDF = spark.sql(s"select b.loginfo from test.flume a lateral view json_tuple(a.logjson, 'logtype', 'loginfo') b as logtype,loginfo  where a.day='$day'and (logtype='WARN' or logtype='ERROR')")
        hiveDF.take(10)
        bcAlertList = BroadcastUtil.getInstance.updateAndGet(spark, bcAlertList)
        val alertInfoList:java.util.List[String] = bcAlertList.value

        import spark.implicits._
        val alerts = hiveDF.mapPartitions(x=>{
            var result = new ArrayBuffer[(String,Int)]()
            while(x.hasNext){
                val loginfo = x.next().getString(0)
                for(alert<-alertInfoList){
                    if(loginfo.contains(alert)){
                        result+=((alert,1))
                    }
                }
            }
            result.iterator
        }).collectAsList().groupBy(_._1).map(x=>(x._1,x._2.size)).toList

        batchInsert(day,alerts)
    }

    def batchInsert(day:String,alerts: List[(String,Int)]) :Unit= {
        NamedDB('loganalysis).localTx { implicit session =>
            for (alert<- alerts){
                SQL("insert into alert_count(keywords,key_count,key_day) values(?,?,?)")
                        .bind(alert._1,alert._2,day)
                        .update().apply()
            }
        }
    }
}
