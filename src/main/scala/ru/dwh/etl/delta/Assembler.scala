package ru.dwh.etl.delta

import java.util.UUID

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Column, Row}
import ru.dwh.etl.delta.TableMetaFactory.{META_KEY_FIELD, META_VALUE_FIELD, NK}

abstract class Assembler(val jobId: String, val suffixNew: String, val suffixOld: String, val partitionKey: String, val partitionValue: String) {
  def meta(): Seq[StructField] = ???

  def newAlias(field: String): String = {
    s"${field}_$suffixNew"
  }

  def oldAlias(field: String): String = {
    s"${field}_$suffixOld"
  }

  def newKeyAlias(): Column = {
    col(newAlias(META_KEY_FIELD))
  }

  def oldKeyAlias(): Column = {
    col(oldAlias(META_KEY_FIELD))
  }

  def assembleNewRow(row: Row): Seq[Any]

  def assembleDeletedRow(row: Row): Seq[Any]

  def assembleStationaryRow(row: Row): Seq[Any]

  def assembleUpdatedRow(row: Row): Seq[Any]

  def assemble(row: Row): Seq[Any] = {
    val newKeyAlias = newAlias(META_KEY_FIELD)
    val newKey = row.getAs[String](newKeyAlias)

    val oldKeyAlias = oldAlias(META_KEY_FIELD)
    val oldKey = row.getAs[String](oldKeyAlias)

    val rr = if (oldKey == null)
      assembleNewRow(row)
    else if (newKey == null)
      assembleDeletedRow(row)
    else {
      val newVal = row.getAs[String](newAlias(META_VALUE_FIELD))
      val oldVal = row.getAs[String](oldAlias(META_VALUE_FIELD))
      if (newVal == oldVal)
        assembleStationaryRow(row)
      else
        assembleUpdatedRow(row)
    }

    rr
  }

  import org.apache.spark.sql.functions._

  private def generateAliases(suffix: String, fields: Seq[StructField]): Seq[Column] = fields.map(f => {
    val name = f.name.toUpperCase()
    val cl = col(name).alias(s"${name}_$suffix")
    cl
  })

  def generateNewAliases(fields: Seq[StructField]): Seq[Column] = {
    generateAliases(suffixNew, fields)
  }

  def generateOldAliases(fields: Seq[StructField]): Seq[Column] = {
    generateAliases(suffixOld, fields)
  }
}


object Assembler {
  def apply(config: Config): Assembler = {
    s"${config.tableType.toLowerCase()}" match {
      case "dim" => new DimAssembler(config.metaJobId, "__src__", "__mir__", config.partitionKey, config.partitionValue())
      case "fact" => new FactAssembler(config.metaJobId, "__src__", "__mir__", config.partitionKey, config.partitionValue())
    }
  }

  class FactAssembler(jobId: String, suffixNew: String, suffixOld: String, partitionKey: String, partitionValue: String) extends Assembler(jobId, suffixNew, suffixOld, partitionKey, partitionValue) {
    override def meta(): Seq[StructField] = Seq(
      TableMetaFactory.META_FIELDS.getOrElse(TableMetaFactory.DWSJOB, null),
      TableMetaFactory.META_FIELDS.getOrElse(TableMetaFactory.DWSAUTO, null),
      TableMetaFactory.META_FIELDS.getOrElse(TableMetaFactory.DWSARCHIVE, null),
      TableMetaFactory.META_FIELDS.getOrElse(TableMetaFactory.DWSACT, null)
    )

    override def assembleNewRow(row: Row): Seq[Any] = {
      val keyHash = row.getAs[String](newAlias(META_KEY_FIELD))
      val valHash = row.getAs[String](newAlias(META_VALUE_FIELD))
      Seq(jobId, "N", "N", "I", keyHash, valHash, partitionValue)
    }

    override def assembleDeletedRow(row: Row): Seq[Any] = {
      val keyHash = row.getAs[String](oldAlias(META_KEY_FIELD))
      val valHash = row.getAs[String](oldAlias(META_VALUE_FIELD))
      val partitionValue = row.getAs[String](oldAlias(partitionKey))

      Seq(jobId, "N", "Y", "D", keyHash, valHash, partitionValue)
    }

    override def assembleStationaryRow(row: Row): Seq[Any] = {
      val keyHash = row.getAs[String](oldAlias(META_KEY_FIELD))
      val valHash = row.getAs[String](oldAlias(META_VALUE_FIELD))
      val partitionValue = row.getAs[String](oldAlias(partitionKey))
      val jobId = row.getAs[String](oldAlias(TableMetaFactory.DWSJOB))

      Seq(jobId, "N", "N", "S", keyHash, valHash, partitionValue)
    }

    override def assembleUpdatedRow(row: Row): Seq[Any] = {
      val keyHash = row.getAs[String](oldAlias(META_KEY_FIELD))
      val valHash = row.getAs[String](newAlias(META_VALUE_FIELD))

      Seq(jobId, "N", "N", "U", keyHash, valHash, partitionValue)
    }
  }

  class DimAssembler(jobId: String, suffixNew: String, suffixOld: String, partitionKey: String, partitionValue: String) extends Assembler(jobId, suffixNew, suffixOld, partitionKey, partitionValue) {
    override def meta(): Seq[StructField] = Seq(
      TableMetaFactory.META_FIELDS.getOrElse(NK, null),
      TableMetaFactory.META_FIELDS.getOrElse(TableMetaFactory.DWSJOB, null),
      TableMetaFactory.META_FIELDS.getOrElse(TableMetaFactory.DWSUNIACT, null),
      TableMetaFactory.META_FIELDS.getOrElse(TableMetaFactory.DWSARCHIVE, null),
      TableMetaFactory.META_FIELDS.getOrElse(TableMetaFactory.DWSACT, null)
    )

    override def assembleNewRow(row: Row): Seq[Any] = {
      val nk = UUID.randomUUID().toString
      val keyHash = row.getAs[String](newAlias(META_KEY_FIELD))
      val valHash = row.getAs[String](newAlias(META_VALUE_FIELD))
      Seq(nk, jobId, null, "N", "I", keyHash, valHash, partitionValue)
    }

    override def assembleDeletedRow(row: Row): Seq[Any] = {
      val keyHash = row.getAs[String](oldAlias(META_KEY_FIELD))
      val valHash = row.getAs[String](oldAlias(META_VALUE_FIELD))
      val partitionValue = row.getAs[String](oldAlias(partitionKey))
      val nk = row.getAs[String](oldAlias(NK))

      Seq(nk, jobId, null, "Y", "D", keyHash, valHash, partitionValue)
    }

    override def assembleStationaryRow(row: Row): Seq[Any] = {
      val keyHash = row.getAs[String](oldAlias(META_KEY_FIELD))
      val valHash = row.getAs[String](oldAlias(META_VALUE_FIELD))
      val partitionValue = row.getAs[String](oldAlias(partitionKey))
      val nk = row.getAs[String](oldAlias(NK))

      Seq(nk, jobId, null, "N", "S", keyHash, valHash, partitionValue)
    }

    override def assembleUpdatedRow(row: Row): Seq[Any] = {
      val keyHash = row.getAs[String](oldAlias(META_KEY_FIELD))
      val valHash = row.getAs[String](newAlias(META_VALUE_FIELD))
      val nk = row.getAs[String](oldAlias(NK))

      Seq(nk, jobId, null, "N", "U", keyHash, valHash, partitionValue)
    }
  }
}
