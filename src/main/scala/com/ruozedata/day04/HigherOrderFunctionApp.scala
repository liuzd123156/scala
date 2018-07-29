package com.ruozedata.day04

import scala.reflect.ClassTag

object HigherOrderFunctionApp {
    def main(args: Array[String]): Unit = {
        val a = List(1,2,3,4,5)
        val b = List("a","b","c","d")
        val c = List("a","b","c")

        println(a)
        /*
            map是对集合中的每个元素进行乘以2操作
            结果都是：List(2, 4, 6, 8, 10)
         */
        println(a.map((x:Int)=>x*2))
        println(a.map(x=>x*2))
        println(a.map(_*2))

        /*
            filter是对集合进行过滤操作，只返回执行为true的结果
            结果：List(3, 4, 5)
         */
        println(a.filter(_>2))

        /*
            flatten是把多维集合打扁成一维集合，集合内包含所有元素
         */
        val str = "a b c d e f w g w w s"
        val tmp = str.split(" ")
        val array = Array(tmp,tmp)
        showArray(array)
        showArray(array.flatten)

        /*
            foreach跟map类似都是针对集合中的每一个元素做处理，只不过foreach没有返回
         */
        array.flatten.foreach(x => print(x +" "))
        println()

        /*
            flatMap大体上适用于二维及以上集合，如果只是需要对其中所有的一维元素做操作，那么等价于flatten
            但是如果操作一维元素得到集合的结果也适用(flatMap(这里要求返回集合))，比如WordCount里Array[String]==>Array[Array[String]]
         */
        array.flatMap(x=>x.filter(_=="a")).foreach(y => print(y +","))
        println()

        /*
            groupBy根据指定的数据进行分组，原数据为A，分组后为Map(key,(A,...,A))
         */
        println(array.flatten.map(x=>(x,2)).groupBy(_._1).map(y => (y._1,y._2.size)))
        println(array.flatten.groupBy(x=>x) map(y => (y._1,y._2.size)))

        /*
            reduce等价于reduceLeft
         */
        println(a.reduceLeft((a,b) => getMax(a,b)))
        println(array.flatten.reduceRight((a,b) => getMax(a,b)))
    }

    def showArray[T:ClassTag](array:Array[T]): Unit ={
        for (x<-array){
            x match {
                case x:Array[String] => showArray(x)
                case y:String => print(y+" ")
                case _ => println("error")
            }
        }
        println()
    }

    def getMax(a:Int,b:Int): Int ={
        println(a + " " +b)
        if(a>b){
            a
        }else{
            b
        }
    }

    def getMax(a:String,b:String): String ={
        println(a + " " +b)
        if(a>b){
            a
        }else{
            b
        }
    }

}