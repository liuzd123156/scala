package com.ruozedata.day02

/**
  * 抽象类示范
  */
object AbstractApp {
    def main(args: Array[String]): Unit = {
        val boy = new Boy
        boy.show
    }
}
abstract class Person{//抽象类abstract关键字，包括至少一个未实现的属性或方法
    var name:String = _
    val age:Int
    def show
}

class Boy extends Person {
    this.name = "1"//
    override val age: Int = 0//抽象类继承实现父类的属性和方法不需要强制加上override

    def show: Unit = {
        println("Boy show "+name+"  "+age)
    }
}