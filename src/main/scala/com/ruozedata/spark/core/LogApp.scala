package com.ruozedata.spark.core

import org.apache.spark.{SparkConf, SparkContext}

object LogApp {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("LogApp").setMaster("local[2]")
        val sc = new SparkContext(sparkConf)

        /*
            需求1：求每个域名的流量统计
            注意：
                1) 流量可能有异常数据，需要特殊处理

         */
        val file = sc.textFile("D:/test.txt")

        val logs = file.map(_.split("\t")).map(x=>{
            val key = x(0)
            var value = 0l
            try{
                value = x(1).trim.toLong
            }catch{
                case e:Exception => e
            }
            (key,value)
        }).reduceByKey(_+_).sortBy(_._2)
        logs.collect().foreach(println)

        /*
            需求2：求每个域名下访问最多的2个资源
         */
        val logs2 = file.map(_.split("\t")).map(x=>{
            val net = x(0)
            val file = x(3)
            var value = 0l
            try{
                value = x(1).trim.toLong
            }catch{
                case e:Exception => e
            }
            ((net,file),value)
        }).reduceByKey(_+_).groupBy(_._1._1).map(_._2.toList.sortBy(_._2).reverse.take(2)).take(10).map(x=>{
            for(y<-x){
                print(y._1,y._2)
            }
            println()
        })
        sc.stop()
    }
}
