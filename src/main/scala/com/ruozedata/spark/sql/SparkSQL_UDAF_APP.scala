package com.ruozedata.spark.sql

//弱类型
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

/*
{"name":"Michael", "salary":3000}
{"name":"Andy", "salary":4500}
{"name":"Justin", "salary":3500}
{"name":"Berta", "salary":4000}
求平均工资
 */

class SparkSQL_UDAF_APP extends UserDefinedAggregateFunction{
    // 输入数据
    override def inputSchema: StructType = StructType(StructField("salary", LongType) :: Nil)

    // 每一个分区中的 共享变量
    override def bufferSchema: StructType = StructType(StructField("sum", LongType) :: StructField("count", IntegerType) :: Nil)

    // 表示UDAF的输出类型
    override def dataType: DataType = DoubleType

    // 表示如果有相同的输入是否存在相同的输出，如果是则true
    override def deterministic: Boolean = true

    // 初始化每个分区中的 共享变量
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0L  // 就是sum
        buffer(1) = 0   // 就是count
    }

    // 每一个分区中的每一条数据  聚合的时候需要调用该方法
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        // 获取这一行中的工资，然后将工资加入到sum中
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        // 将工资的个数加1
        buffer(1) = buffer.getInt(1) + 1
    }

    // 将每一个分区的输出合并，形成最后的数据
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        // 合并总的工资
        buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
        // 合并总的工资个数
        buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
    }

    // 给出计算结果
    override def evaluate(buffer: Row): Any = {
        // 取出总的工资 / 总工资个数
        buffer.getLong(0).toDouble / buffer.getInt(1)
    }
}

object SparkSQL_UDAF_APP {
    def main(args: Array[String]): Unit = {
        val sparkConf = new SparkConf().setAppName("udaf").setMaster("local[*]")
        val spark = SparkSession
                .builder()
                .config(sparkConf)
                .getOrCreate()
        val employee = spark.read.json("employee.json")

        employee.createOrReplaceTempView("employee")

        spark.udf.register("average", new SparkSQL_UDAF_APP)

        spark.sql("select average(salary) from employee").show()

        spark.stop()
    }
}


//强类型
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}

case class Employee(name: String, salary: Long)

case class Aver(var sum: Long, var count: Int)

class Average extends Aggregator[Employee, Aver, Double] {

    // 初始化方法 初始化每一个分区中的 共享变量
    override def zero: Aver = Aver(0L, 0)

    // 每一个分区中的每一条数据聚合的时候需要调用该方法
    override def reduce(b: Aver, a: Employee): Aver = {
        b.sum = b.sum + a.salary
        b.count = b.count + 1
        b
    }

    // 将每一个分区的输出 合并 形成最后的数据
    override def merge(b1: Aver, b2: Aver): Aver = {
        b1.sum = b1.sum + b2.sum
        b1.count = b1.count + b2.count
        b1
    }

    // 给出计算结果
    override def finish(reduction: Aver): Double = {
        reduction.sum.toDouble / reduction.count
    }

    // 主要用于对共享变量进行编码
    override def bufferEncoder: Encoder[Aver] = Encoders.product

    // 主要用于将输出进行编码
    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble

}

object Average{

    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setAppName("udaf").setMaster("local[*]")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._

        val employee = spark.read.json("D:\\JetBrains\\workspace\\sparkcore\\sparksql\\src\\main\\resources\\employee.json").as[Employee]

        val aver = new Average().toColumn.name("average")

        employee.select(aver).show()

        spark.stop()
    }

}
