package com.ruozedata.day04.work

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

/** 参考博客：
  *     https://blog.csdn.net/qq_31780525/article/details/79036728
  *     统计单词出现次数
  *     输出结果：List((hello,100), (welcome,100), (world,86), (welcomehello,72))
  */
object WordCount {
    def main(args: Array[String]): Unit = {
        val file = Source.fromFile("D:/hive-wc.txt")
        val lineIterator = file.getLines()
        var array = new ArrayBuffer[String]
        for (x <- lineIterator){
            array ++= x.split(",")
        }
        array.map(x => (x,1)).groupBy(_._1).map(y => (y._1,y._2.size)).mkString("  ")

        println(array.map(x => (x,1)).groupBy(_._1).map(y => (y._1,y._2.size)).toList.sortBy(x=>x._2).reverse)
        //map(x => (x,1)) 返回结果：ArrayBuffer((hello,1),...,(welcome,1))
        //groupBy(_._1) 返回结果：Map(welcome -> ArrayBuffer((welcome,1),...,(welcome,1)),...,hello -> ArrayBuffer((hello,1),...,(hello,1)))
        //map(y => (y._1,y._2.size)) 返回结果：Map(welcome -> 100, world -> 86, welcomehello -> 72, hello -> 100)
        //toList 输出结果：List((welcome,100), (world,86), (welcomehello,72), (hello,100))
        //sortBy(x=>x._2) 输出结果：List((welcomehello,72), (world,86), (welcome,100), (hello,100))
        //reverse 输出结果：List((hello,100), (welcome,100), (world,86), (welcomehello,72))
    }
}
