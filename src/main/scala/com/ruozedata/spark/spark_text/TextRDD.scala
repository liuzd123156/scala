package com.ruozedata.spark.spark_text

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types.StructType

private class TextRDD(
sc: SparkContext,
schema: StructType,
columns: Array[String],
filters: Array[Filter],
partitions: Array[Partition],
url: String,
options: TextOptions) extends RDD[InternalRow](sc, Nil){
    override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = ???

    override protected def getPartitions: Array[Partition] = partitions
}
