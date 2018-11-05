package com.ruozedata

import java.text.SimpleDateFormat
import java.util.Date

import scala.util.Random

object HelloWorld {
  def main(args:Array[String]){
      for(i<-1 to 100){
          println(getRandomInt(3))
      }
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
