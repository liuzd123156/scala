package com.ruozedata.day03

/*
    class和object的区别、联系示例
 */
object ObjectClassApp {
    def main(args: Array[String]): Unit = {
//        println(new ApplyTest().arg2) //这里无法访问arg2属性，因为这个属性是私有的
        val applyTest = ApplyTest
        val applyTest2 = ApplyTest()
        println(applyTest())
        println(applyTest2())
    }
}

object ApplyTest{
    val arg1:String = "测试属性1"
    println(new ApplyTest().arg2)//这里可以访问arg2属性，因为该object和class同名且在同一scala文件中，互为半生关系，可以互相访问对方任意属性和方法
    def apply(): ApplyTest = {
        println("object ApplyTest.apply running ...")
        new ApplyTest()
    }
}
class ApplyTest{
    private val arg2 = "测试属性2"
    println(ApplyTest.arg1)
    def apply(): Unit = {
      println("class ApplyTest.apply running ...")
    }
}
