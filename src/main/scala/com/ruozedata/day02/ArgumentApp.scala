package com.ruozedata.day02
/*
    参数相关示范
 */
object ArgumentApp {
    def main(args: Array[String]): Unit = {
        method1()//打印结果：默认参数
        method1("覆盖默认参数")//打印结果：覆盖默认参数

        method2("默认参数1","默认参数2")//打印结果：默认参数1 : 默认参数2
        method2(arg2 = "命名参数1",arg1 = "命名参数2")//打印结果：命名参数2 : 命名参数1

        method3(List("string1","string2","string3","string4"):_*)//打印结果：string1string2string3string4
    }
    /*
        参数可以有默认值，如果不传则用默认值，传了则覆盖默认值
     */
    def method1(arg1:String="默认参数"): Unit ={
        println(arg1)
    }

    /*
        参数可以以key=value形式，key是参数名称
     */
    def method2(arg1:String,arg2:String): Unit ={
        println(arg1+" : "+arg2)
    }

    /*
        变长参数
     */
    def method3(args:String*): Unit ={
        var result:String = ""
        for (arg<-args){
            result += arg
        }
        println(result)
    }
}
