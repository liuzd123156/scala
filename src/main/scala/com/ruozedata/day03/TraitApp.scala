package com.ruozedata.day03

import org.apache.spark.internal.Logging

/**
  * trait和abstract类似
  * 1) trait修饰的对象除了第一个是用extends继承，其他的都是用with
  * 2) 一个类可以继承多个trait对象，但是只能继承一个abstract，所以优先使用trait
  * 3) 如果需要使用带参数的构造函数，则必须使用abstract，因为trait不能使用带参数构造函数
  */
object TraitApp {
    def main(args: Array[String]): Unit = {
//        val appTest = new AppTest("")
    }
}

class AppTest extends TraitAppTest with Logging{
    override val arg: String = ""

    override def test: Unit = {

    }
}

trait TraitAppTest{
    val arg:String
    def test
}