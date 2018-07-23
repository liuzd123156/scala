package com.ruozedata

import scalikejdbc._
import scalikejdbc.config.DBs

case class User(id: Int, name: String, age: Int)
object ScalaJDBC {

  def main(args: Array[String]): Unit = {

    //解析application.conf的文件。
    DBs.setup('ruozedata)
    //和DBs.setup('default)等价

    val userList:List[User] = List(User(101,"zhangsan",21),User(102,"lisi",22))
    println("开始新增:"+batchSave(userList))
    println("开始查询:")
    val users = select()
    for (user <- users){
      println("id:"+user.id +" name:"+user.name+" age:"+user.age)
    }

    println("开始更新:"+update())
    println("开始删除101:"+deleteByID(101))
    println("开始删除102:"+deleteByID(102))
  }


  def deleteByID(id:Int) = {
    NamedDB('ruozedata).autoCommit { implicit session =>
      SQL("delete from user where id = ?").bind(id).update().apply()
    }
  }


  def update() {
    NamedDB('ruozedata).autoCommit { implicit session =>
      SQL("update user set age = ? where id = ?").bind(23, 101).update().apply()
    }
  }

  //select查询到数据之后会产生一个rs的对象集，然后可以得到这个对象集里面的数据。
  def select():List[User] = {
    NamedDB('ruozedata).readOnly { implicit session =>
      SQL("select * from user").map(rs => User(rs.int("id"), rs.string("name"), rs.int("age"))).list().apply()
    }
  }

  def batchSave(users:List[User]) :Unit= {
    NamedDB('ruozedata).localTx { implicit session =>
      for (user<- users){
        SQL("insert into user(name,age,id) values(?,?,?)").bind(user.name, user.age, user.id).update().apply()
      }
    }
  }

}
