package com.ruozedata.day02

/**
  * 构造器使用示范
  */
object ConstructApp {
    def main(args: Array[String]): Unit = {
        val animal = new Animal("构造器","主")
        animal.speak()
        /**
          * 输出结果：
          * 主 construct in ...
            主 construct out ...
            主构造器初始化
          */

        val animal2 = new Animal("构造器","主","附属1")
        animal2.speak()
        /**
          * 输出结果：
          * 主 construct in ...
            主 construct out ...
            附属1 construct in ...
            附属1 construct out ...
            主附属1构造器初始化
          */

        val animal3 = new Animal("构造器","主","附属1","附属2")
        animal3.speak()

        /**
          * 输出结果：
            主 construct in ...
            主 construct out ...
            附属1 construct in ...
            附属1 construct out ...
            附属2 construct in ...
            附属2 construct out ...
            主附属1附属2构造器初始化
          */
    }
}

//主构造函数，新建该对象时必须传入这里指定的参数,如果没有var和val修饰,就不会有默认的get和set生产,即没法通过类名.属性的方式使用
class Animal(name:String,tp:String){
    println("主 construct in ...")
    def speak(): Unit ={
        println(tp + name +"初始化")
    }

    def this(name:String,tp:String,test:Int){//test作为没有在类中定义的属性，所以没法当成字段使用
        this(tp+name,tp)
        println(test)
    }

    var tp1:String = _//类中的其他属性，附属构造器可以使用,该属性有var修饰，所以可以通过类名.属性的方式直接访问
    def this(name:String,tp:String,tp1:String){
        this(tp1+name,tp)//附属构造器的第一行必须调用主构造器
        println(s"${tp1} construct in ...")
        this.tp1 = tp1
        println(s"${tp1} construct out ...")
    }

    var tp2:String = _
    def this(name:String,tp:String,tp1:String,tp2:String){
        this(tp2+name,tp,tp1)
        println(s"${tp2} construct in ...")
        this.tp2 = tp2
        println(s"${tp2} construct out ...")
    }
    println("主 construct out ...")
}