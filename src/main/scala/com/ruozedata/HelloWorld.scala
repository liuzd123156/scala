package com.ruozedata

object HelloWorld {
  def main(args:Array[String]){
    println("Hello ruozedata ....")
    println(add(2,5))
    println(add("2","5"))
    println(1 to 10)
    println(sum(1 to 10:_*))
  }
  def add(a:Int,b:Int):Int = {
    a+b
  }
  def add(a:String,b:String): String ={
    a+b
  }

  def sum(numbers:Int*)={
    var result = 0
    for (number<-numbers) {
      result += number
    }
    result
  }
}
