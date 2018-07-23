package com.ruozedata.day02

object ToUntilRangeApp {
    def main(args: Array[String]): Unit = {
        /*
            to的三种使用方式，结果都是Range(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
         */
        println(1 to 10)
        println(1.to(10))
        println(1 to(10))

        /*
            until的三种使用方式，结果都是Range(1, 2, 3, 4, 5, 6, 7, 8, 9)，左闭右开
         */
        println(1 until 10)
        println(1.until(10))
        println(1 until(10))

        /*

         */
        println(Range())
    }
}
