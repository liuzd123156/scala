package com.ruozedata.day02

/*
    类继承示范，方法的重写（继承的重写）和重载（已有的重载）也有体现
    输出结果：
    zhangsan 20 0.0
    lisi 22 F 1.0
    lisi 22 F
 */
object ClassExtendApp {
    def main(args: Array[String]): Unit = {
        val people = new People("zhangsan",20)
        people.show()

        val girl = new Girl("lisi",22,"F")
        girl.show()
        girl.show("")
    }
}
class People(val name:String,val age:Int){//添加修饰符val、var的参数会默认生成get和set方法，new对象之后可以通过对象.属性的方式操作使用属性
    val heigth:Double = 0
    def show(): Unit ={
        println(name+" "+age+" "+heigth)
    }
}

class Girl(name:String,age:Int,val sex:String) extends People(name,age){
    override val heigth:Double = 1//重写属性和方法时都需要override关键字
    override def show(): Unit = {//重写继承于父类的方法，如果不重写，则默认调用父类的该方法（方法名一样，参数也一样）
        println(name+" "+age+" "+sex+" "+heigth)
    }

    def show(test:String): Unit = {//重载方法，方法名一样，参数不一样（参数个数或参数类型不一样）
        println(name+" "+age+" "+sex)
    }
}