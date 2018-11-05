package org.apache.spark.sql.spark_text_night

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{CodecStreams, OutputWriter}
import org.apache.spark.sql.types._

class EmpWriter(
                 path: String,
                 dataSchema: StructType,
                 context: TaskAttemptContext,
                 option: EmpOptions) extends OutputWriter with Logging with Serializable{

  private val writer = CodecStreams.createOutputStreamWriter(context, new Path(path))

  override def write(row: InternalRow): Unit = {

    /*
      My do
      There's a big hole here.
      If the InternalRow of certain fields was used as a partition fields, they were pulling away from the InternalRow.
      This will affect the InternalRow subscript read if pull away.
      It wasted me for three hours,to find the bugs look deserialization failure.
     */

    var i = 0;
    val textBuilder:StringBuilder = new StringBuilder(1000);
    for(dt <- dataSchema){
      textBuilder.append(row.get(i,dt.dataType).toString).append(",")
      i+= 1
    }

    writer.write(textBuilder.substring(0,textBuilder.length-1)+"\n")
  }

  override def close(): Unit = writer.close()
}
