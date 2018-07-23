package com.ruozedata.day02

object MethodApp {
    /*
        def  方法名(参数列表):返回结果类型={方法体}，如果有返回值，需放到最后一行
     */
    def main(args: Array[String]): Unit = {
        method1()//无参数可以带括号访问或者不带括号访问
        method1
        
        method3("参数1")
    }

    def method1(): Unit ={//等价于def method1(){}    def method1() ={}
        println("不带参数，不带返回结果方法")
    }
    //重载同一方法名，不同参数
    def method1(arg:String): Unit ={//等价于def method1(arg:String){}   def method1(arg:String)={}
        println("带参数，不带返回结果方法")
    }

    def method2(): String ={ //等价于 def method2(){}   def method2()={}  def method2()="返回结果"
        println("不带参数，带返回结果方法")
        "返回结果"
    }

    def method3(arg:String): String ={//等价于def method1(arg:String){}   def method1(arg:String)={}
        println("带参数，带返回结果方法")
        "返回结果 ："+arg
    }
}
