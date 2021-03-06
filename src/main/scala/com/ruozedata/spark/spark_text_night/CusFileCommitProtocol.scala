package org.apache.spark.sql.spark_text_night

import java.util.{Date, UUID}

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.spark.internal.Logging
import org.apache.spark.internal.io.{FileCommitProtocol, HadoopMapReduceCommitProtocol, SparkHadoopWriterUtils}
import org.apache.spark.mapred.SparkHadoopMapRedUtil

import scala.collection.mutable
import scala.util.Try

class CusFileCommitProtocol(jobId: String,path: String,dynamicPartitionOverwrite: Boolean = false)
  extends FileCommitProtocol
    with Serializable with Logging{

  import FileCommitProtocol._

  /** OutputCommitter from Hadoop is not serializable so marking it transient. */
  @transient private var committer: OutputCommitter = _

  /**
    * Checks whether there are files to be committed to a valid output location.
    *
    * As committing and aborting a job occurs on driver, where `addedAbsPathFiles` is always null,
    * it is necessary to check whether a valid output path is specified.
    * [[HadoopMapReduceCommitProtocol#path]] need not be a valid [[org.apache.hadoop.fs.Path]] for
    * committers not writing to distributed file systems.
    */
  private val hasValidPath = Try { new Path(path) }.isSuccess

  /**
    * Tracks files staged by this task for absolute output paths. These outputs are not managed by
    * the Hadoop OutputCommitter, so we must move these to their final locations on job commit.
    *
    * The mapping is from the temp output path to the final desired output path of the file.
    */
  @transient private var addedAbsPathFiles: mutable.Map[String, String] = null

  /**
    * Tracks partitions with default path that have new files written into them by this task,
    * e.g. a=1/b=2. Files under these partitions will be saved into staging directory and moved to
    * destination directory at the end, if `dynamicPartitionOverwrite` is true.
    */
  @transient private var partitionPaths: mutable.Set[String] = null

  /**
    * The staging directory of this write job. Spark uses it to deal with files with absolute output
    * path, or writing data into partitioned directory with dynamicPartitionOverwrite=true.
    */
  private def stagingDir = new Path(path, ".spark-staging-" + jobId)

  protected def setupCommitter(context: TaskAttemptContext): OutputCommitter = {
    val format = context.getOutputFormatClass.newInstance()
    // If OutputFormat is Configurable, we should set conf to it.
    format match {
      case c: Configurable => c.setConf(context.getConfiguration)
      case _ => ()
    }
    format.getOutputCommitter(context)
  }

  override def newTaskTempFile(taskContext: TaskAttemptContext, dir: Option[String], ext: String): String = {
    //val filename = getFilename(taskContext, ext)


    /*
     * My do
     *    Read the filename from the configuration , instead of the default random filename
     *    In append mode, the partition files are automatically merged into one-file
     */

    val filename = taskContext.getConfiguration.get("filename","data.data")

    val stagingDir: Path = committer match {
      case _ if dynamicPartitionOverwrite =>
        assert(dir.isDefined,
          "The dataset to be written must be partitioned when dynamicPartitionOverwrite is true.")
        partitionPaths += dir.get
        this.stagingDir
      // For FileOutputCommitter it has its own staging path called "work path".
      case f: FileOutputCommitter =>
        new Path(Option(f.getWorkPath).map(_.toString).getOrElse(path))
      case _ => new Path(path)
    }

    dir.map { d =>
      new Path(new Path(stagingDir, d), filename).toString
    }.getOrElse {
      new Path(stagingDir, filename).toString
    }
  }

  override def newTaskTempFileAbsPath(taskContext: TaskAttemptContext, absoluteDir: String, ext: String): String = {
     val filename = getFilename(taskContext, ext)

    val absOutputPath = new Path(absoluteDir, filename).toString

    // Include a UUID here to prevent file collisions for one task writing to different dirs.
    // In principle we could include hash(absoluteDir) instead but this is simpler.
    val tmpOutputPath = new Path(stagingDir, UUID.randomUUID().toString() + "-" + filename).toString

    addedAbsPathFiles(tmpOutputPath) = absOutputPath
    tmpOutputPath
  }

  private def getFilename(taskContext: TaskAttemptContext, ext: String): String = {
    // The file name looks like part-00000-2dd664f9-d2c4-4ffe-878f-c6c70c1fb0cb_00003-c000.parquet
    // Note that %05d does not truncate the split number, so if we have more than 100000 tasks,
    // the file name is fine and won't overflow.
    val split = taskContext.getTaskAttemptID.getTaskID.getId
    f"part-$split%05d-$jobId$ext"
  }

  override def setupJob(jobContext: JobContext): Unit = {
    // Setup IDs
    val jobId = SparkHadoopWriterUtils.createJobID(new Date, 0)
    val taskId = new TaskID(jobId, TaskType.MAP, 0)
    val taskAttemptId = new TaskAttemptID(taskId, 0)

    // Set up the configuration object
    jobContext.getConfiguration.set("mapreduce.job.id", jobId.toString)
    jobContext.getConfiguration.set("mapreduce.task.id", taskAttemptId.getTaskID.toString)
    jobContext.getConfiguration.set("mapreduce.task.attempt.id", taskAttemptId.toString)
    jobContext.getConfiguration.setBoolean("mapreduce.task.ismap", true)
    jobContext.getConfiguration.setInt("mapreduce.task.partition", 0)

    val taskAttemptContext = new TaskAttemptContextImpl(jobContext.getConfiguration, taskAttemptId)
    committer = setupCommitter(taskAttemptContext)
    committer.setupJob(jobContext)
  }

  override def commitJob(jobContext: JobContext, taskCommits: Seq[TaskCommitMessage]): Unit = {
    committer.commitJob(jobContext)

    val filename = jobContext.getConfiguration.get("filename","data.data")

    if (hasValidPath) {
      val (allAbsPathFiles, allPartitionPaths) =
        taskCommits.map(_.obj.asInstanceOf[(Map[String, String], Set[String])]).unzip
      val fs = stagingDir.getFileSystem(jobContext.getConfiguration)

      val filesToMove = allAbsPathFiles.foldLeft(Map[String, String]())(_ ++ _)
      logDebug(s"Committing files staged for absolute locations $filesToMove")
      if (dynamicPartitionOverwrite) {
        val absPartitionPaths = filesToMove.values.map(new Path(_).getParent).toSet
        logDebug(s"Clean up absolute partition directories for overwriting: $absPartitionPaths")
        absPartitionPaths.foreach(fs.delete(_, true))
      }
      for ((src, dst) <- filesToMove) {
        fs.rename(new Path(src), new Path(dst))
      }

      if (dynamicPartitionOverwrite) {
        val partitionPaths = allPartitionPaths.foldLeft(Set[String]())(_ ++ _)
        logDebug(s"Clean up default partition directories for overwriting: $partitionPaths")
        for (part <- partitionPaths) {
          val stagingDirPath = new Path(stagingDir, part)
          val finalPartPath = new Path(path, part)
          if (!fs.exists(finalPartPath.getParent)) {
            fs.mkdirs(finalPartPath.getParent)
          }

           /*
            * My do
            *   In the append mode, the data will be appended to the original file.
            *   So change the append strategy here to maintain idempotency
            *   The way to do this is to simply delete the original file and keep everything else.
            *   In this way, only the result of this calculation is retained each time, and the result of other calculations will not be affected
            *   The standard for identifying different calculation-times is the filename from the configuration
            */
          val finalFilePath = new Path(finalPartPath,filename)
          if(fs.exists(finalFilePath)){
            fs.delete(finalFilePath,false)
          }
          //Copy the result from the temporary folder
          fs.rename(new Path(stagingDirPath, filename), new Path(finalPartPath, filename))
        }
      }

      fs.delete(stagingDir, true)
    }
  }

  override def abortJob(jobContext: JobContext): Unit = {
    committer.abortJob(jobContext, JobStatus.State.FAILED)
    if (hasValidPath) {
      val fs = stagingDir.getFileSystem(jobContext.getConfiguration)
      fs.delete(stagingDir, true)
    }
  }

  override def setupTask(taskContext: TaskAttemptContext): Unit = {
    committer = setupCommitter(taskContext)
    committer.setupTask(taskContext)
    addedAbsPathFiles = mutable.Map[String, String]()
    partitionPaths = mutable.Set[String]()
  }

  override def commitTask(taskContext: TaskAttemptContext): TaskCommitMessage = {
    val attemptId = taskContext.getTaskAttemptID
    SparkHadoopMapRedUtil.commitTask(
      committer, taskContext, attemptId.getJobID.getId, attemptId.getTaskID.getId)
    new TaskCommitMessage(addedAbsPathFiles.toMap -> partitionPaths.toSet)
  }

  override def abortTask(taskContext: TaskAttemptContext): Unit = {
    committer.abortTask(taskContext)
    // best effort cleanup of other staged files
    for ((src, _) <- addedAbsPathFiles) {
      val tmp = new Path(src)
      tmp.getFileSystem(taskContext.getConfiguration).delete(tmp, false)
    }
  }
}
