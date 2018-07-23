package com.ruozedata.day02

/*
    to  until  Range使用示范
 */
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
            Range(arg1,arg2,arg3)：arg1是开始，arg2是结束，arg3是步长，左闭右开
            Range(1,10)输出结果：Range(1, 2, 3, 4, 5, 6, 7, 8, 9)
            Range(1,10,1)输出结果：Range(1, 2, 3, 4, 5, 6, 7, 8, 9)
            Range(1,10,2)输出结果：Range(1, 3, 5, 7, 9)
         */
        println(Range(1,10))
        println(Range(1,10,1))
        println(Range(1,10,2))
    }
}
