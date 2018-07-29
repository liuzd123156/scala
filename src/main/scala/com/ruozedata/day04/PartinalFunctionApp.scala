package com.ruozedata.day04

/*
    偏函数：被包在花括号内没有match的一组case语句
 */
object PartinalFunctionApp {
    def main(args: Array[String]): Unit = {
        def pf:PartialFunction[Int,String] = {
            case 1 => "a"
            case 2 => "b"
//            case _ => "c"
        }
        println(pf(1))
        println(pf(2))
        println(pf.isDefinedAt(3))
    }
}
