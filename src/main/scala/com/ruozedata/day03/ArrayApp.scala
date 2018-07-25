package com.ruozedata.day03


import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag
/*
    Array和ArrayBuffer示例
 */
object ArrayApp {
    def main(args: Array[String]): Unit = {
        /*
            定长数组演示
         */
        val a = new Array[String](5)//定长数组声明方式1
        a(0)="1"
        val b = Array("string1","string2")//定长数组声明方式2，常用方式
        b.update(1,"STRING2")//修改指定位置的元素，默认位置是从0开始
        b.length

        //mkString(String arg1,String arg2,String arg3) arg1：字符串开始字符 arg2：数组元素分割字符 arg3：字符串结束字符
        b.mkString//输出结果:string1STRING2
        b.mkString(",")//输出结果：string1,STRING2
        b.mkString("(",",",")")//输出结果:(string1,STRING2)
        println(b.mkString("(",",",")"))

        for(i<-0 until (b.length)){//遍历方法1
            println(b(i))
        }

        for(x<-b){//遍历方法2
            println(x)
        }

        b.foreach(println)//遍历方法3

        /*
            变长数组演示
         */
        var c = new ArrayBuffer[Int]()
        c += 1//+=添加一个元素1
        c += 2
        c += 3
        c += 4
        showArrayBuffer(c)//输出结果：1 2 3 4
        c += (5,6,7)//添加多个元素，以tuple(元祖)的形式
        showArrayBuffer(c)//输出结果：1 2 3 4 5 6 7
        c ++= Array(8,9)//变长+定长，++实现
        showArrayBuffer(c)//输出结果：1 2 3 4 5 6 7 8 9
        c.insert(0,0)//数组的第一位插入一个原始0
        showArrayBuffer(c)//输出结果：0 1 2 3 4 5 6 7 8 9
        c.insert(10,c:_*)//把0 1 2 3 4 5 6 7 8 9从第十个位置插入
        showArrayBuffer(c)//输出结果：0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9

        c.remove(0)//删除第1个元素
        showArrayBuffer(c)//输出结果：1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9
        c.remove(10,2)//从第11个开始删除，删除两个
        showArrayBuffer(c)//输出结果：1 2 3 4 5 6 7 8 9 0 3 4 5 6 7 8 9
        c.trimEnd(2)//从数组尾部删除2个
        showArrayBuffer(c)//输出结果：1 2 3 4 5 6 7 8 9 0 3 4 5 6 7
        c -= 3//删除第一个等于3的元素，删除后就结束,即使有多个也只删除第一个
        showArrayBuffer(c)//输出结果：1 2 4 5 6 7 8 9 0 3 4 5 6 7
        c -= 3
        showArrayBuffer(c)//输出结果：1 2 4 5 6 7 8 9 0 4 5 6 7
        c --= Array(2,4,5)
        showArrayBuffer(c)//输出结果：1 6 7 8 9 0 4 5 6 7
//      val array = c.toArray//把变长数组转成定长数组
        println("max:"+c.max)
        println("min:"+c.min)

    }

    def showArrayBuffer[T:ClassTag](array:ArrayBuffer[T]): Unit ={
        for (x<-array){
            print(x+" ")
        }
        println()
    }

}
