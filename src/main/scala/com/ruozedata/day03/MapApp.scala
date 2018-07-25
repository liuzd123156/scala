package com.ruozedata.day03

import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

object MapApp {
    def main(args: Array[String]): Unit = {
        /*
            定长Map
         */
        val a = Map("a"->1,"b"->2,"c"->3)
        println(a("a"))
        println(a.getOrElse("a",0))//推荐使用这个，如果key对应的有值，就返回该值，否则返回默认的0
        println(a.mkString(","))
        showMap(a)
        for(x<-a.keySet){
            print(a(x)+" ")
        }
        println()
        for(x<-a.values){
            print(x+" ")
        }
        println()
        for((x,y)<-a){
            print(x +":"+y+"  ")
        }
        println()
        //        showMap(a.updated("a",1))
        /*
            变长Map
         */
        val c = new HashMap[String,Int]
        c.put("aa",1)//等价于c("a")=1
        c("bb")=2
        c += ("cc"->3)
        c ++= a
        showMapHash(c)
        c.remove("a")
        showMapHash(c)

    }

    def showMap[T:ClassTag,A:ClassTag](map:Map[T,A]): Unit ={
        for (x<- map){
            print(x + " ")
        }
        println()
    }

    def showMapHash[T:ClassTag,A:ClassTag](map:HashMap[T,A]): Unit ={
        for (x<- map){
            print(x + " ")
        }
        println()
    }
}
