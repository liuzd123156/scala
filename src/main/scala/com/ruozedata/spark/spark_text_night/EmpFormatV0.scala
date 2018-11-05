package org.apache.spark.sql.spark_text_night

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.mapreduce
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.CompressionCodecs
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SerializableConfiguration

class EmpFormatV0 extends TextBasedFileFormat with Serializable {



  private  val columnTypes = Array(
    StructField("emp_no", IntegerType, false), // 0
    StructField("year", IntegerType, false), // 1
    StructField("month", IntegerType, false), // 2
    StructField("day", IntegerType, false), // 3
    StructField("ename", StringType, true), // 4
    StructField("job", StringType, true), // 5
    StructField("mgr", IntegerType, true), // 6
    StructField("hire_date", StringType, true), // 7
    StructField("sal", FloatType, true), // 8
    StructField("comm", FloatType, true), // 9
    StructField("dept_no", IntegerType, true)) // 10

  /*
    My do
    Specify the emp file , no infer and use the fixed-schema
   */
  override def inferSchema(sparkSession: SparkSession, options: Map[String, String], files: Seq[FileStatus]): Option[StructType] = {
    Some(StructType(columnTypes))
  }

  override def buildReader(
                            sparkSession: SparkSession,
                            dataSchema: StructType,
                            partitionSchema: StructType,
                            requiredSchema: StructType,
                            filters: Seq[Filter],
                            options: Map[String, String],
                            hadoopConf: Configuration): PartitionedFile => Iterator[InternalRow] = {
    val empOptions = new EmpOptions(options)
    val broadcastedHadoopConf =
      sparkSession.sparkContext.broadcast(new SerializableConfiguration(hadoopConf))

    (file: PartitionedFile) => {
      val confValue = broadcastedHadoopConf.value.value
      val reader = if (!empOptions.wholeText) {
        new HadoopFileLinesReader(file, confValue)
      } else {
        new HadoopFileWholeTextReader(file, confValue)
      }
      Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => reader.close()))
      reader.map { line =>

        /*
          My do
          String-ling to InternalRow
         */

        val row = try {
          val columns = line.toString.split("\t",8)
          val time = columns(4).split("-")
          Array(columns(0).toInt,
            time(0).toInt,
            time(1).toInt,
            time(2).toInt,
            UTF8String.fromString(columns(1)),
            UTF8String.fromString(columns(2)),
            columns(3).toInt,
            UTF8String.fromString(columns(4)),
            columns(5).toFloat,
            columns(6).toFloat,
            columns(7).toInt)
        }catch {
          case _:Exception => Array(-1,0,0,0,UTF8String.fromString(""),UTF8String.fromString(""),0,UTF8String.fromString(""),0.0F,0.0F,0)
        }
        new  GenericInternalRow(row)
      }
    }
  }

  override def prepareWrite(sparkSession: SparkSession, job: Job, options: Map[String, String], dataSchema: StructType): OutputWriterFactory = {
    val empOptions = new EmpOptions(options)
    val conf = job.getConfiguration

    empOptions.compressionCodec.foreach { codec =>
      CompressionCodecs.setCodecConfiguration(conf, codec)
    }

    new OutputWriterFactory {

      override def getFileExtension(context: mapreduce.TaskAttemptContext): String = {
        ".txt" + CodecStreams.getCompressionExtension(context)
      }

      override def newInstance(path: String, dataSchema: StructType, context: mapreduce.TaskAttemptContext): OutputWriter = {
        new EmpWriter(path, dataSchema, context,empOptions)
      }
    }
  }
}
