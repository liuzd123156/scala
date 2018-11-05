package com.ruozedata.spark.core

//import java.io.{File, PrintWriter}

import org.apache.spark.{SparkConf, SparkContext}

object SparkCoreHomeworkApp {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setMaster("local[2]").setAppName("test")
        val sc = new SparkContext(sparkConf)

        /**
          * 根据员工入职年份分别写到不同文件夹中
          * Linux测试没有问题，但是windows测试失败
          */
        val sourceFile = "file:///home/hadoop/data/emp.txt"
//        val targetPath = args(1)
        val rdd = sc.textFile(sourceFile).map(x=>{
            val line = x.split("\t")
            val year = line(4).split("-")(0)
            (year,x)//把数据的年份摘出来，当成key
        })
        val key = rdd.keys.distinct().collect()//求不重复的key列表
        key.foreach(x=>{
            rdd.filter(y=>x==y._1).coalesce(1,true).saveAsTextFile("file:///home/hadoop/data/ruoze/emp/y="+x)
        })
        sc.stop()
    }
}
