package com.ruozedata.spark.spark_text_night

import org.apache.spark.sql.SparkSession

object CusDataSourceAppTest extends App{

    val spark = SparkSession.builder()
            .appName("CusDataSourceAppTest")
            .master("local[1]")
            .config("spark.sql.sources.commitProtocolClass","com.ruozedata.spark.spark_text_night.CusFileCommitProtocol")
            .getOrCreate()

    val df =  spark.read.format("com.ruozedata.spark.spark_text_night.EmpFormatV0").load("D:\\data\\emp.txt")
    df.show(false)
    /*
  df.show(false)
  +------+----+-----+---+------+---------+----+----------+-------+------+-------+
  |emp_no|year|month|day|ename |job      |mgr |hire_date |sal    |comm  |dept_no|
  +------+----+-----+---+------+---------+----+----------+-------+------+-------+
  |7369  |1980|12   |17 |SMITH |CLERK    |7902|1980-12-17|800.0  |0.0   |20     |
  |7499  |1981|2    |20 |ALLEN |SALESMAN |7698|1981-2-20 |1600.0 |300.0 |30     |
  |7521  |1981|2    |22 |WARD  |SALESMAN |7698|1981-2-22 |1250.0 |500.0 |30     |
  |7566  |1981|4    |2  |JONES |MANAGER  |7839|1981-4-2  |2975.0 |0.0   |20     |
  |7654  |1981|9    |28 |MARTIN|SALESMAN |7698|1981-9-28 |1250.0 |1400.0|30     |
  |7698  |1981|5    |1  |BLAKE |MANAGER  |7839|1981-5-1  |2850.0 |0.0   |30     |
  |7782  |1981|6    |9  |CLARK |MANAGER  |7839|1981-6-9  |2450.0 |0.0   |10     |
  |7788  |1987|4    |19 |SCOTT |ANALYST  |7566|1987-4-19 |3000.0 |0.0   |20     |
  |7839  |1981|11   |17 |KING  |PRESIDENT|0   |1981-11-17|5000.0 |0.0   |10     |
  |7844  |1981|9    |8  |TURNER|SALESMAN |7698|1981-9-8  |1500.0 |0.0   |30     |
  |7876  |1987|5    |23 |ADAMS |CLERK    |7788|1987-5-23 |1100.0 |0.0   |20     |
  |7900  |1981|12   |3  |JAMES |CLERK    |7698|1981-12-3 |950.0  |0.0   |30     |
  |7902  |1981|12   |3  |FORD  |ANALYST  |7566|1981-12-3 |3000.0 |0.0   |20     |
  |7934  |1982|1    |23 |MILLER|CLERK    |7782|1982-1-23 |1300.0 |0.0   |10     |
  |8888  |1988|1    |23 |HIVE  |PROGRAM  |7839|1988-1-23 |10300.0|0.0   |0      |
  +------+----+-----+---+------+---------+----+----------+-------+------+-------+
     */

    //df.show(false)


    df.write
            .partitionBy("year")
            .format("com.ruozedata.spark.spark_text_night.EmpFormatV0")
            .mode("append")
            .option("filename","from_cus.txt")
            .save("D:\\data\\test\\CusDataSourceOutput")

    spark.stop()
}

