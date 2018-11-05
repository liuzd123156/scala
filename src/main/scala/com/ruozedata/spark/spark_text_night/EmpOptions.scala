package org.apache.spark.sql.spark_text_night

import org.apache.spark.sql.catalyst.util.{CaseInsensitiveMap, CompressionCodecs}

class EmpOptions(@transient private val parameters: CaseInsensitiveMap[String]) extends Serializable {
  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  val compressionCodec = parameters.get("compression").map(CompressionCodecs.getCodecClassName)

  val wholeText = parameters.getOrElse("wholetext", "false").toBoolean

  private val EMP_PATH = "path"
  require(parameters.isDefinedAt(EMP_PATH), s"Option '$EMP_PATH' is required.")
  val path = parameters(EMP_PATH)

  private val EMP_OUTPUT_COALESCE = "output_coalesce"
  val output_coalesce = parameters.getOrElse(EMP_OUTPUT_COALESCE,"1").toInt


}
