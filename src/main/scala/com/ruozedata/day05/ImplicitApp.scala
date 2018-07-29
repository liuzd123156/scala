package com.ruozedata.day05

import scala.reflect.ClassTag

object ImplicitApp {
    def main(args: Array[String]): Unit = {
        //值隐式转换，需要把初始化和取值定义在同一作用域，否则无意义
        implicit val name:String = "implicit"//如果没有定义隐士值下面的println会报错：could not find implicit value for parameter name: String
//        implicit val name2:String = "implicit2"//如果重复定义，也会报错，因为没法区分
        //所以隐式值转换需要满足无歧义规则，在声明隐式参数的类型是最好使用特别的或自定义的数据类型
        //不要使用Int,String这些常用类型，避免碰巧匹配
        def person(implicit name : String) = name
        println(person)

        //隐式类型转换，就是把一个类型转成另一种类型，可以用来纯粹的类型转换或者为了使用另一类型的某个方法（比如给旧的对象增加新方法）
        printString("x")
        printString(1)//输出结果：int2String function 1
        printString(1.1)//输出结果：double2String function 1.1
        printString(Array(1,2,3,4))//输出结果：array2String function 1,2,3,4

        implicit def ImplicitOld2New(ia:ImplicitOld): ImplicitNew ={
            new ImplicitNew
        }
        val old = new ImplicitOld()
        old.newFunction()
    }
    def printString(x:String): Unit ={//该方法只接受String类型的输入
        println(x)
    }

    implicit def int2String(x:Int): String ={//Int转换成String
        print("int2String function ")
        x.toString
    }
    implicit def double2String(x:Double): String ={//Double转换成String
        print("double2String function ")
        x.toString
    }
    implicit def array2String[T:ClassTag](x:Array[T]): String ={//Array转换成String
        print("array2String function ")
        x.mkString(",")
    }
}

class ImplicitNew{
    def newFunction(): Unit ={
        println("this is a new function(ImplicitApp have no)")
    }

}
class ImplicitOld{
    def oldFunction(): Unit ={
        println("this is a old function")
    }
}
