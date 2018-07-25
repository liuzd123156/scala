package com.ruozedata.day03

import scala.reflect.ClassTag
import scala.collection.mutable.HashSet

/*
    集合使用示例
 */
object SetApp {
    def main(args: Array[String]): Unit = {
        /*
            定长Set
         */
        val c = Set(1)
        val a = Set(1,2,3,4,5)
        showSet(a)//输出结果：5 1 2 3 4
        val b = Set(1,3,5,7,9,9)
        showSet(b)//输出结果：5 1 9 7 3 ,去除了重复的9，自动去重
        showSet(a.union(b))//两个集合的并集，等价于a ++ b和a | b
//        a ++ b
        showSet(b.intersect(a))//两个集合的交集，等价于b&a 输出结果：5 1 3
        showSet(b.diff(a))//差集，获取b中a没有的元素的集合 输出结果：9 7
        showSet(a.diff(b))//输出结果：2 4 等价于 a&~b和a -- b
        println(a.subsetOf(b))//输出结果：false 判断a是否是b的子集
        println(c.subsetOf(b))//输出结果：true 判断c是否是b的子集

        /*
            变长Set(Set默认实现方式是HashSet，Sort)
         */
        val h = new HashSet[Int]()
        h += 1
        h += 2
        h += 3
        showSetHash(h)
        h += (4,5,6)
        showSetHash(h)
        h ++= HashSet(7,8,9)
        showSetHash(h)
        h -= 1
        showSetHash(h)
        h.remove(2)//删除元素2，等价于 h -= 2
        showSetHash(h)
//        val iterator = h.iterator//集合遍历方法之一
//        while (iterator.hasNext){
//            println(iterator.next())
//        }

    }

    def showSet[T:ClassTag](set:Set[T]): Unit ={
        for(x<-set){
            print(x+" ")
        }
        println()
    }

    def showSetHash[T:ClassTag](set:HashSet[T]): Unit ={
        for(x<-set){
            print(x+" ")
        }
        println()
    }

}
