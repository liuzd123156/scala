package com.ruozedata.spark.spark_text

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.types._
import org.apache.spark.Partition

private[spark_text] class TextRelation(parts:Array[Partition],textOptions:TextOptions)(@transient val sparkSession: SparkSession)
        extends BaseRelation with PrunedFilteredScan with InsertableRelation{
    override def sqlContext: SQLContext = sparkSession.sqlContext

    override def schema: StructType = StructType(
        Array(
            StructField("empno",IntegerType,true),
            StructField("name",StringType,true),
            StructField("position",StringType,true),
            StructField("leader",StringType,true),
            StructField("birth",StringType,true),
            StructField("year",StringType,true),
            StructField("sal",DoubleType,true),
            StructField("deptno",IntegerType,true)
        )
    )

    override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = ???

    override def insert(data: DataFrame, overwrite: Boolean): Unit = ???
}
