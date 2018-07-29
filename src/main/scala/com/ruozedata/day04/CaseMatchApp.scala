package com.ruozedata.day04

/*
    模式匹配示例
 */
object CaseMatchApp {
    def main(args: Array[String]): Unit = {
        //简单模式匹配
        val str = "dc"
        val result = str match {
            case "a" => 1//如果str==a,返回1
            case "b" => 2//如果str==b,返回2
            case _ if isRight(str,"c") => 3//守卫可以case方法
            case _ => 0  //如果str既不等于a，也不等于b，则返回0
        }
        println(result)
        //参数模式匹配
        isRight("","1") match {
            case sh if isRight("true",""+sh) => println(true)
            case _ => println(false)
        }
        //类型模式匹配
        val str2:Any = 2.1
        str2 match {
            case s: String => println("this is string " + s)
            case x: Int => println("this is integer " + x)
//            case Double => println("java.lang.Double")
            case ch => println("this is other " + ch.getClass)
        }
        //数组匹配
        def arrayMatch(arr:Array[String]) = arr match {
            case Array("Hello") => println("the array only contain 'Hello'")
            case Array(x,y) => println("the array contain two value " + x + " and " + y)
            case Array(x,_*) => println("the array contain many values " + arr.mkString(","))
            case _ => println("the other array")
        }
        arrayMatch(Array("Hello")) // the array only contain 'Hello'
        arrayMatch(Array("Hello", "World")) // the array contain two value Hello and World
        arrayMatch(Array("Hello", "World", "Yoona")) // the array contain many values Hello,World,Yoona

        //列表匹配（List比较特殊，拥有空的Nil，可以用::运算符，不过这种写法意义不大）
        def listMatch(list:List[String]) = list match {
            case "Hello" :: Nil => println("the list only contain 'Hello'")
            case x :: y :: Nil => println("the list contain two value " + x + " and " + y)
            case "Hello" :: tail => println("the list contain many values " + list)//首个元素是Hello的List
            case _ => println("the other list")
        }
        listMatch(List("Hello")) // the list only contain 'Hello'
        listMatch(List("Hello", "World")) // the list contain two value Hello and World
        listMatch(List("Hello", "World", "Yoona")) // the list contain many values List(Hello, World, Yoona)

        //元组匹配
        def tupleMatch(t:Any) = t match {
            case ("Hello", _) => println("the first value is 'Hello'")
            case (x, "Hello") => println("the first value is " + x + " and the second value is 'Hello'")
            case _ => println("the other tuple")
        }
        tupleMatch(("Hello", "World")) // the first value is 'Hello'
        tupleMatch(("World", "Hello")) // the first value is World and the second value is 'Hello'
        tupleMatch(("Hello", "World", "Yoona")) // the other tuple

    }
    def isRight(str:String,str2:String): Boolean ={
        if(str.contains(str2)){
            true
        }else{
            false
        }
    }
}
