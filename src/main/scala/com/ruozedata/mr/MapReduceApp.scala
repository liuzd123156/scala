package com.ruozedata.mr

import java.io.{BufferedReader, File, InputStreamReader, PrintWriter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

object MapReduceApp {
    def main(args: Array[String]): Unit = {
        val conf = new Configuration()
        conf.set("fs.defaultFS", "hdfs://hadoop01:9000/")
        val fs = FileSystem.get(conf)
        // Move a single file
        val ok = fs.open(new Path("/data/hive-wc.txt"))
        val is = new BufferedReader(new InputStreamReader(ok))
        var line = is.readLine()
        var i = 1
        var array = ArrayBuffer[(String,Int)]()
        val context = new Context
        val empmap = new EmpMap
        val empReduce = new EmpReduce
        while (line!=null){
            empmap.map(i+"",line,context)
            i+=1
            line = is.readLine()
        }
        val keys = context.getKes()
        for(key<-keys){
            empReduce.reduce(key,context)
        }
        context.show()


    }

    class EmpMap extends Mapper{
        override def map(key: String, line: String, context: Context): Unit = {
            val words = line.split(",")
            for(word<-words){
                context.write(word,1)
            }
        }
    }

    class EmpReduce extends Reducer{
        override def reduce(key: String, context: Context): Unit = {
            var result = new mutable.HashMap[String,Int]()
            var sum = 0
            for(value <- context.getTempData(key)){
                sum+=value
            }
            context.remove(key)
            context.write(key,sum)
        }
    }

    def reduce(array:ArrayBuffer[(String,Int)]): Map[String,Int]={
        array.groupBy(_._1).map(x=>(x._1,x._2.size))
    }
}
abstract class Mapper() {
    def map(key:String,line:String,context: Context)
}

abstract class Reducer (){
    def reduce(key:String,context: Context)
}

class Context{
    private var tempData = new mutable.HashMap[String,ArrayBuffer[Int]]()
    def write(key:String,value:Int): Unit ={
        if(tempData.get(key)!=None){
            val keyArray = tempData.getOrElse(key,null)
            keyArray+=value
        }else{
            val keyArray = new ArrayBuffer[Int]()
            keyArray+=value
            tempData.put(key,keyArray)
        }
    }

    def show(): Unit ={
        tempData.foreach(println)
    }

    def getTempData(key:String):ArrayBuffer[Int] ={
        tempData.getOrElse(key,null)
    }

    def remove(key:String): Unit ={
        tempData.remove(key)
    }

    def getKes(): Iterable[String] ={
        tempData.keys
    }
}