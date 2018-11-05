package com.ruozedata.spark.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/*
    包括内容有简单的spark-sql测试、作业(反射实现和API实现)
 */
object SparkSQLApp_functions {
    def main(args: Array[String]): Unit = {
        val spark = SparkSession
                .builder().master("local[2]")
                .appName("SparkSQLApp_functions")
                .getOrCreate()
        val peopleDF = spark.read.format("json").load("file:///D:/people.json")
        peopleDF.show
        peopleDF.printSchema()
        import spark.implicits._
        peopleDF.drop('name).show()
        //三种等价，都是查询name列
        peopleDF.select("name").show()//String类型的ColumnName
        peopleDF.select('name,'age+1).show()
        peopleDF.select($"name",$"age"+1).show()//隐士转换成名称为name的Column

        //过滤
        peopleDF.filter($"age">20).show()
        peopleDF.filter("age>20").show()
        peopleDF.filter("name like 'M%'").show()
        peopleDF.filter("SUBSTR(name,0,1)='M'").show()

        //排序,默认升序,需要Column对象点desc,ColumnName是字符串,没法降序
        peopleDF.sort($"name").show()
        peopleDF.sort($"name".desc).show()
        peopleDF.sort('name.desc).show()
        //先按照名字降序排列，然后再按照年龄升序排列
        peopleDF.sort($"name".desc,'age).show()

        /*
            第三个参数默认 `inner`.其他有(下方join测试写法，演示使用):
   *                 `inner`, `cross`, `outer`, `full`, `full_outer`, `left`, `left_outer`,
   *                 `right`, `right_outer`, `left_semi`, `left_anti`
         */
//        peopleDF.join(peopleDF,peopleDF.col("id")===peopleDF.col("id"),"inner")

        peopleDF.createOrReplaceTempView("people")
        val sqlDF = spark.sql("select * from people")
        sqlDF.show()


        /**
          * 反射方式把RDD转成DataFrame
          */
        val empRDD = spark.sparkContext.textFile("file:///D:/emp.txt")
        val empDF = empRDD.map(_.split("\t")).map(x=>{
//            print(x(0)+" ")
//            print(x(1)+" ")
//            print(x(2)+" ")
//            print(x(3)+" ")
//            print(x(4)+" ")
//            print(x(5)+" ")
//            print(x(6)+" ")
//            println(x(7)+" ")
            var empno,deptno = 0
            var sal,sal2 = 0d;
            try{
                empno = x(0).trim.toInt
            }catch{
                case e:Exception=> e
            }
            try{
                sal = x(5).trim.toDouble
            }catch{
                case e:Exception=> e
            }
            try{
                sal2 = x(6).trim.toDouble
            }catch{
                case e:Exception=> e
            }
            try{
                deptno = x(7).trim.toInt
            }catch{
                case e:Exception=> e
            }
            EMP(empno,x(1),x(2),x(3),x(4),x(4).split("-")(0),sal,sal2,deptno)
        }).toDF()

        empDF.printSchema()
        empDF.show()

        //windows执行有些问题，但是Linux测试成功,
        //注意：format还支持其他格式：csv、text、parquet等，但是text的输出只能保存一列数据
//        empDF.write.mode("overwrite").format("json").partitionBy("year").save("file:///home/hadoop/ruoze/emp/")


        /*
            Row方式把rdd转成DataFrame
         */
        val fields = StructType(Array(
            StructField("empno",IntegerType,true),
            StructField("name",StringType,true),
            StructField("position",StringType,true),
            StructField("leader",StringType,true),
            StructField("birth",StringType,true),
            StructField("year",StringType,true),
            StructField("sal",DoubleType,true),
            StructField("sal2",DoubleType,true),
            StructField("deptno",IntegerType,true)
        ))

        val rowRDD = empRDD.map(_.split("\t")).map(x=>{
            var empno,deptno = 0
            var sal,sal2 = 0d;
            try{
                empno = x(0).trim.toInt
            }catch{
                case e:Exception=> e
            }
            try{
                sal = x(5).trim.toDouble
            }catch{
                case e:Exception=> e
            }
            try{
                sal2 = x(6).trim.toDouble
            }catch{
                case e:Exception=> e
            }
            try{
                deptno = x(7).trim.toInt
            }catch{
                case e:Exception=> e
            }
            Row(empno,x(1),x(2),x(3),x(4),x(4).split("-")(0),sal,sal2,deptno)
        })
        val empDF2 = spark.createDataFrame(rowRDD,fields);
        empDF2.printSchema()
        empDF2.show()

        Thread.sleep(1000000)

        spark.stop()
    }
}
case class EMP(empno:Int,name:String,position:String,leader:String,birth:String,year:String,sal:Double,sal2:Double,deptno:Int)
