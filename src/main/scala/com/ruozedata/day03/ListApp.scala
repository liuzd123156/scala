package com.ruozedata.day03

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/*
    List和ListBuffer示例
 */
object ListApp {
    def main(args: Array[String]): Unit = {
        /*
            定长List
         */
        val list = Nil//Nil是自带的空的List集合
        showList(list)
        val l1 = 1::Nil//1和空List相加=List(1)
        showList(l1)
        val l2 = 2::l1//2和List l1相加=List(2,1),等价于List = List.head::List.tail
        showList(l2)
        val a = List(1,2,3,4,5)
        val c = a.updated(0,0)//update和Array不一样，这个不是在原本的对象实例中更新，而是把更新的结果返回创建一个新的对象实例
        showList(c)
        for(x<-a){
            print(x)
        }
        println()

        for(i<-0 until a.length){
            print(a(i))
        }
        println()
        println(a.head)//输出List的第一个元素
        showList(a.tail)//a.tail返回除了第一个元素之外的其他元素组成的List集合

        /*
           变长List
         */
        val b = new ListBuffer[Int]()//List的操作跟Array类似
        b += 1
        b += 2
        b += 3
        showListBuffer(b)
        b += (4,5,6)
        showListBuffer(b)
        b ++= List(7,8,9)
        showListBuffer(b)
        b.insert(0,0)
        showListBuffer(b)
        b.insert(10,b:_*)
        showListBuffer(b)
        b -=2
        showListBuffer(b)
        b -=2
        showListBuffer(b)
        b --= List(3,5)
        showListBuffer(b)
        b.remove(0)
        b.remove(0,2)
        showListBuffer(b)
    }

    def showListBuffer[T:ClassTag](list:ListBuffer[T]): Unit ={
        for(x<-list){
            print(x+" ")
        }
        println()
    }

    def showList[T:ClassTag](list:List[T]): Unit ={
        for(x<-list){
            print(x+" ")
        }
        println()
    }
}
