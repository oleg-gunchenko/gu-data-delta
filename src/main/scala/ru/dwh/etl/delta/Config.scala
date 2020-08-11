package ru.dwh.etl.delta

import java.util.UUID

import org.apache.commons.lang3.StringUtils

case class Config(hashClass: String = "murmur", sourcePath: String = "", sourceTable: String = "",
                  mirrorPath: String = "", deltaPath: String = "",
                  table: String = "", tableType: String = "dim", date: String = "",
                  partitionKey: String = "export_date", clPartitionValue: String = "",
                  keys: String = "ID", exclusions: String = "",
                  keyBufferSize: Int = 4, valBufferSize: Int = 4096,
                  mode: String = "delta", inline: Boolean = true) {
  val metaJobId: String = UUID.randomUUID().toString

  def partitionValue(): String = if (StringUtils.isEmpty(this.clPartitionValue)) this.metaJobId else this.clPartitionValue

  def getKeySet(): Set[String] = keys.split(",").map(_.toLowerCase()).toSet
  def getExclusionSet(): Set[String] = exclusions.split(",").map(_.toLowerCase()).toSet
  def getPartitionKeySet(): Set[String] = partitionKey.split(",").map(_.toLowerCase()).toSet
}
