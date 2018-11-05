package com.ruozedata.day03.work

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Date

import scala.util.Random


object FileWrite {
    def main(args: Array[String]): Unit = {
        val writer = new PrintWriter(new File("D:/test.txt"))
        for(i<-1 to 10000){
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
        val net_id = getRandomInt(3)
        val result = net(net_id)+"\t"+getRandomInt(5000)+error+"\t"+sdf.format(date)+"\t"+net(net_id)+"_"+getRandomInt(50)
        result
    }

    def getRandomInt(n:Int): Int ={
        Random.nextInt(n)
    }


}
