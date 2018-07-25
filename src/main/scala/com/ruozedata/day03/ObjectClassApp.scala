package com.ruozedata.day03

/*
    class和object的区别、联系示例
    1) object修饰的对象不需要new对象，可以直接使用,class修饰的需要new才可以使用（互为伴生的apply方法除外）
    2) object中的属性和方法都是静态的，类似于java里的static
 */
object ObjectClassApp {
    def main(args: Array[String]): Unit = {
//        println(new ApplyTest().arg2) //这里无法访问arg2属性，因为这个属性是私有的
        val applyTest = ApplyTest
        println("------------")
        /*
        输出结果：调用的是object
                object ApplyTest
         */

        val applyTest2 = ApplyTest()
        println("------------")
        /*
        输出结果：调用的是object.apply方法里的new class
                object ApplyTest.apply running ...
                class ApplyTest
         */

        val applyTest3 = new ApplyTest()//等价于new ApplyTest
        println("------------")
        /*
         输出结果：
                class ApplyTest
         */

        println(applyTest())
        println("------------")
        /*
        输出结果：定义好的object实例加括号等价于ApplyTest(),即对于object对象名和对象实例来说，括号就等于apply方法的隐式调用
                object ApplyTest.apply running ...
                class ApplyTest
                com.ruozedata.day03.ApplyTest@18be83e4
         */

        println(applyTest2())
        println("------------")
        /*
        输出结果：对于定义好的class实例来说，括号就等于class的apply方法隐式调用，class对象加括号不影响
                class ApplyTest.apply running ...
                ()
         */

        println(applyTest3())
        println("------------")
        /*
        输出结果：
                class ApplyTest.apply running ...
                ()
         */
    }
}

object ApplyTest{
    val arg1:String = "测试属性1"
    println("object ApplyTest")
//    println(new ApplyTest().arg2)//这里可以访问arg2属性，因为该object和class同名且在同一scala文件中，互为半生关系，可以互相访问对方任意属性和方法
    def apply(): ApplyTest = {
        println("object ApplyTest.apply running ...")
        new ApplyTest()
    }
}
class ApplyTest{
    private val arg2 = "测试属性2"
    println("class ApplyTest")
//    println(ApplyTest.arg1)
    def apply(): Unit = {
      println("class ApplyTest.apply running ...")
    }
}
