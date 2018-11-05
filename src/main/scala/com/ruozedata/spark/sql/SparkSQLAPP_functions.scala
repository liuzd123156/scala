package com.ruozedata.spark.sql

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/*
    包括内容有简单的functions测试、作业(反射实现和API实现)
 */
object SparkSQLApp {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession
                .builder().master("local[2]")
                .appName("SparkSQLApp")
                .getOrCreate()

        import spark.implicits._

        val empRDD = spark.sparkContext.textFile("file:///D:/emp.txt")
        val empDF = empRDD.map(_.split("\t")).map(x=>{
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

//        empDF.groupBy("deptno")

        //自定义方法
//        spark.udf.register("str_length",(x:String)=>x.length)
//        empDF.createOrReplaceTempView("emp")
//        empDF.filter("sal>2000").createOrReplaceTempView("emp")
//        spark.sql("select empno,name,str_length(name) as lname from emp")

        import org.apache.spark.sql.functions._
        val resultDF = empDF.select($"empno",$"name",$"position",$"leader",$"birth",$"year",$"sal",$"sal2",$"deptno",(row_number().over(Window.partitionBy("deptno").orderBy($"sal".desc))).as("rank")).filter("rank<4")
        resultDF.write.mode("overwrite").format("json").partitionBy("deptno").save("file:///home/hadoop/data/tmp/emp")
        spark.stop()
    }

}
