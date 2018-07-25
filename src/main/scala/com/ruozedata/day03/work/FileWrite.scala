package com.ruozedata.day03.work

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import scala.util.Random


object FileWrite {
    def main(args: Array[String]): Unit = {
        val writer = new PrintWriter(new File("D:/test.txt"))
        for(i<-1 to 1000){
            if(i%100==0){
                writer.println(mkLine("'"))
            }else{
                writer.println(mkLine(""))
            }
        }
        writer.close()
    }

    def mkLine(error:String):String={
        val net = Array("www.ruozedata.com","www.zhibo8.com","www.dongqiudi.com")
        var sdf = new SimpleDateFormat("'['yyyy-MM-dd HH:mm:ss']'")
        val date = new Date()
        val result = net(getRandomInt(3))+"\t"+getRandomInt(5000)+error+"\t"+sdf.format(date)
        result
    }

    def getRandomInt(n:Int): Int ={
        Random.nextInt(n)
    }


}
