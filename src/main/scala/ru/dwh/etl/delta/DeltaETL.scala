package ru.dwh.etl.delta

import java.io.{File, PrintWriter}
import java.util.UUID

import org.apache.commons.io.FileUtils.forceDelete
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.{SparkConf, SparkContext}
import ru.dwh.etl.delta.TableMetaFactory.{DWSACT, META_KEY_FIELD}
import scopt.OParser

import scala.util.matching.Regex

object DeltaETL {
  val AVRO_FORMAT = "com.databricks.spark.avro"

  val SEPARATOR: Byte = ';'.toByte

  def prepareHash(session: SparkSession, sourceDF: DataFrame, config: Config, tableMeta: TableMeta): DataFrame = {
    val keys = config.getKeySet().map(_.toLowerCase())
    val exclusions = config.getExclusionSet().map(_.toLowerCase())

    val rkeys = sourceDF.schema.map(_.name).filter(f => keys.contains(f.toLowerCase)).toSet
    val rexcl = sourceDF.schema.map(_.name).filter(f => exclusions.contains(f.toLowerCase)).toSet

    val newHashedRDD = sourceDF.rdd.mapPartitions(it => {
      val tx = Transformations(config, rkeys, rexcl)
      it.map(row => tx.addHashes(row))
    })

    session.createDataFrame(newHashedRDD, DataTypes.createStructType(tableMeta.hash_payload().toArray))
  }

  def storeDF(df: DataFrame, path: String): Unit = {
    storeDF(df, AVRO_FORMAT, path)
  }

  def storeDF(df: DataFrame, format: String, path: String): Unit = {
    try {
      forceDelete(new File(path))
    } catch {
      case e: Exception =>
    }

    df.write
      .format(format)
      .mode("overwrite")
      .save(path)
  }

  def extractDelta(config: Config, session: SparkSession, mirror: DataFrame, source: DataFrame, tableMeta: TableMeta, assembler: Assembler): (DataFrame, DataFrame) = {
    val sourceMetaCols = tableMeta.hash_payload()

    val jobId = UUID.randomUUID().toString

    val newAliases = assembler.generateNewAliases(sourceMetaCols)
    val sm = source.select(newAliases: _*)

    val oldMetaFields = tableMeta.meta_table()
    val om = oldMetaFields.filter(f => !f.name.toLowerCase.equalsIgnoreCase(DWSACT))

    val oldAliases = assembler.generateOldAliases(om)

    val mm = mirror.select(oldAliases: _*)

    val nka = assembler.newKeyAlias()
    val oka = assembler.oldKeyAlias()

    val smm = sm.join(mm, nka === oka, "outer")

    val joinedMetaRDD: RDD[Row] = smm.rdd.mapPartitions(p => {
      val asm = Assembler(config)
      p.map(r => Row.fromSeq(asm.assemble(r)))
    })

    val joinedMetaTmp = session.createDataFrame(joinedMetaRDD, DataTypes.createStructType(tableMeta.meta_table().toArray))

    val tmpPath = s"/tmp/${UUID.randomUUID().toString}"

    storeDF(joinedMetaTmp, tmpPath)

    val joinedMeta = loadDf(session, tmpPath)

    val newDFMeta = joinedMeta.where(joinedMeta(DWSACT) === lit("I") || joinedMeta(DWSACT) === lit("U"))
    val oldDFMeta = joinedMeta.where(joinedMeta(DWSACT) === lit("D") || joinedMeta(DWSACT) === lit("S"))

    val kPayloadColumns = TableMetaFactory.columns(tableMeta.hash_payload())

    val vSource = source.select(kPayloadColumns: _*)
    val vMirror = mirror.select(kPayloadColumns: _*)

    val nuDF = newDFMeta.join(vSource, META_KEY_FIELD)
    val dsDF = oldDFMeta.join(vMirror, META_KEY_FIELD)

    val inserted = nuDF.where(nuDF(DWSACT) === lit("I"))
    val updated = nuDF.where(nuDF(DWSACT) === lit("U"))

    val deleted = dsDF.where(dsDF(DWSACT) === lit("D"))
    val stationary = dsDF.where(dsDF(DWSACT) === lit("S"))

    val dc = TableMetaFactory.columns(tableMeta.meta_table())

    val deltaDF = nuDF.union(deleted).select(dc: _*)

    val mirrorDF = inserted.union(updated).union(stationary).select(dc: _*).drop(DWSACT)

    (deltaDF, mirrorDF)
  }

  def loadDf(session: SparkSession, path: String): DataFrame = {
    val df = session.read
      .format(AVRO_FORMAT)
      .load(path)
    df
  }

  implicit class RegexOps(sc: StringContext) {
    def r = new Regex(sc.parts.mkString, sc.parts.tail.map(_ => "x"): _*)
  }

  def createHiveTableDefinition(config: Config, suffix: String, sf: Seq[StructField]): String = {
    var format = "orc"

    val imf = sf.filter(f => !((f.name.toLowerCase == TableMetaFactory.DWSJOB.toLowerCase) || (f.name.toLowerCase == DWSACT.toLowerCase)))
      .map(f => s"${f.name} ${f.dataType.sql}")
      .mkString(",")

    s"create table if not exists ${config.table}$suffix ($imf) partitioned by (${TableMetaFactory.DWSJOB} string) stored as $format;"
  }

  def createHiveTable(session: SparkSession, sqlText: String): Unit = {
    session.sqlContext.sql(sqlText)
  }

  def storeDelta(df: DataFrame, table: String, partitionKey: String): DataFrame = {
    val tbl = s"${table}_delta"
    df.write
      .partitionBy(partitionKey.toLowerCase)
      .mode(SaveMode.Append)
      .format("orc")
      .saveAsTable(tbl)
    df.sqlContext.sparkSession.catalog.refreshTable(tbl)
    df.sparkSession.sqlContext.sql(s"refresh table $tbl")
    df
  }

  def storeMirror(df: DataFrame, table: String, partitionKey: String): DataFrame = {
    df.write
      .partitionBy(partitionKey)
      .mode(SaveMode.Overwrite)
      .format("orc")
      .saveAsTable(table)
    df.sparkSession.sqlContext.sql(s"refresh table $table")
    df
  }

  def sparkSession(): SparkSession = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    sc.setCheckpointDir(s"/tmp/${UUID.randomUUID().toString}")

    val session = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    session
  }

  def dropMirrorTable(config: Config, tempTable: String) {
    val supSession = sparkSession()
    val table = if (config.inline) config.table else s"${config.table}_nklink"
    supSession.sql(s"drop table $table")
    supSession.sql(s"create table $table using orc partitioned by (${TableMetaFactory.DWSJOB}) as select * from $tempTable")
    supSession.sql(s"drop table $tempTable")
    supSession.close()
  }

  def checkFieldsExists(df: DataFrame, keys: Set[String]): Boolean = {
    var hasAllIds: Boolean = true
    keys.foreach(k => hasAllIds &= df.schema.fields.exists(f => f.name == k))
    hasAllIds
  }

  def checkTableExistence(session: SparkSession, tableName: String): DataFrame = {
    try {
      session.sqlContext.sql(s"select * from $tableName")
    } catch {
      case ex: Exception =>
        System.out.println(ex)
        throw ex
    }
  }

  def createMirrorSql(config: Config, session: SparkSession, tableMeta: TableMeta): String = {
    val keys = config.getKeySet()
    val partKeys = config.getPartitionKeySet()

    val common = keys ++ partKeys

    val cond = common.map(k => s"s.`$k` = n.`$k`").mkString(" and ")

    checkTableExistence(session, s"${config.table}_src")

    val metaFields = (tableMeta.meta() ++ tableMeta.kv_hash()).map(_.name).filter(_ != DWSACT).map(k => s"n.`$k` as `$k`").mkString(", ")
    val payloadFields = tableMeta.payload_metaex().map(_.name).map(k => s"s.`$k` as `$k`").mkString(", ")

    val result =
      s"""create or replace view ${config.table} as
         |select $metaFields, $payloadFields
         |from ${config.table}_src s
         |     join ${config.table}_nklink n on $cond""".stripMargin

    result
  }

  def buildDelta(config: Config): Unit = {
    val session = sparkSession()

    val source = loadDf(session, config.sourcePath)

    val tableMeta = TableMetaFactory(config, source)

    val name = config.table.split("\\.")
    val ns = name.head
    val tempTable = s"$ns.__${UUID.randomUUID().toString.replace('-', '_')}"

    val hashClass = config.hashClass.toLowerCase()
    val assembler = Assembler(config)

    try {
      val mirror = checkTableExistence(session, config.table)

      val nColumns = TableMetaFactory.columns(tableMeta.payload())
      val narrowedSource = source.select(nColumns: _*)

      val hashedSource = prepareHash(session, narrowedSource, config, tableMeta)

      var (delta: DataFrame, newMirror: DataFrame) = extractDelta(config, session, mirror, hashedSource, tableMeta, assembler)

      storeDelta(delta, config.table, TableMetaFactory.DWSJOB)
      storeMirror(newMirror, tempTable, TableMetaFactory.DWSJOB)
      session.close()
      dropMirrorTable(config, tempTable)
    } catch {
      case ex: Throwable =>
        System.out.println(ex.getMessage)
        session.close()
    } finally {
    }
  }

  def checkTableSanity(df: DataFrame, keys: Set[String], partKeys: Set[String]): Boolean = {
    checkFieldsExists(df, keys) && checkFieldsExists(df, partKeys)
  }

  def createDDL(config: Config): Unit = {
    val session = sparkSession()

    val df = if (config.inline)
      loadDf(session, config.sourcePath)
    else
      checkTableExistence(session, config.sourceTable)

    val tableMeta = TableMetaFactory(config, df)

    val buffer: StringBuilder = new StringBuilder

    if (config.inline) {
      buffer.append(createHiveTableDefinition(config, "", tableMeta.meta_table()))
    } else {
      /*
            buffer.append(createHiveTableDefinition(config, "_delta", tableMeta.meta_table()))
            buffer.append(";")
            buffer.append("\n")
      */
      buffer.append(createHiveTableDefinition(config, "_nklink", tableMeta.meta_table()))
      buffer.append("\n")
      buffer.append(createMirrorSql(config, session, tableMeta))
      buffer.append(";")
      buffer.append("\n")
    }

    var out = new PrintWriter(new File(s"${config.table}.ddl"))
    out.println(buffer.toString())

    out.flush()
    out.close()

    session.close()
  }

  def addHivePartition(config: Config): Unit = {
    val session = sparkSession()

    val table = config.table
    val key = config.partitionKey
    val value = config.clPartitionValue
    val path = config.sourcePath

    val ddl = s"""alter table $table add if not exists partition($key='$value') location '$path'"""
    session.sqlContext.sql(ddl)
    session.sqlContext.sql(s"refresh table $table")

    session.close()
  }

  def main(args: Array[String]): Unit = {
    /*
        val cfg: Option[Config] = OParser.parse(cmdLineParser(), args, Config())
        val config: Config = cfg.orNull
    */
    val config: Config = Config(inline = false, keys = "ID,T1",
      partitionKey = "yy,mn,dy",
      hashClass = "md5",
      sourcePath = "/user/hive/warehouse/etl.db/test2_src/year=2020/month=12/day=5/", sourceTable = "etl.test2_src",
      exclusions = "created,updated",
      table = "etl.test2", tableType = "dim", mode = "delta")

    config.mode match {
      case "ddl" => createDDL(config)
      case "delta" => buildDelta(config)
      case "add-partition" => addHivePartition(config)
    }
  }

  def cmdLineParser(): OParser[Unit, Config] = {

    val builder = OParser.builder[Config]

    val cmdLineParser = {

      import builder._

      OParser.sequence(
        programName("Naumen Delta Extractor"),
        head("Naumen"),
        cmd("ddl")
          .action((_, c) => c.copy(mode = "ddl"))
          .children(
            opt[String]("source-path")
              .required()
              .action((x, c) => c.copy(sourcePath = x)),
            opt[String]("table-type")
              .required()
              .action((x, c) => c.copy(tableType = x)),
            opt[String]("table")
              .optional()
              .action((x, c) => c.copy(table = x))
              .text("table should be full path to a table e.g SCHEMA.TABLE"),
            opt[String]("partition-key")
              .optional()
              .action((x, c) => c.copy(partitionKey = x))
              .text("partition-key should be string value")
          ),
        cmd("add-partition")
          .action((_, c) => c.copy(mode = "add-partition"))
          .children(
            opt[String]("table")
              .optional()
              .action((x, c) => c.copy(table = x))
              .text("table should be full path to a table e.g SCHEMA.TABLE"),
            opt[String]("source-path")
              .optional()
              .action((x, c) => c.copy(sourcePath = x))
              .text("path to part"),
            opt[String]("partition-key")
              .optional()
              .action((x, c) => c.copy(partitionKey = x))
              .text("partition-key should be string value"),
            opt[String]("partition-value")
              .optional()
              .action((x, c) => c.copy(clPartitionValue = x))
              .text("partition-value key should be string value")),
        cmd("delta")
          .action((_, c) => c.copy(mode = "delta"))
          .children(
            opt[String]("hash-class")
              .action((x, c) => c.copy(hashClass = x))
              .text("hash-class should be one of MURMUR, MD5 OR SHA"),
            opt[String]("keys")
              .action((x, c) => c.copy(keys = x))
              .text("keys should be comma separated fields of primary key"),
            opt[Int]("key-buffer")
              .action((x, c) => c.copy(keyBufferSize = x))
              .text("key-buffer should be value in KB"),
            opt[Int]("val-buffer")
              .action((x, c) => c.copy(valBufferSize = x))
              .text("val-buffer should be value in KB"),
            opt[String]("exclusions")
              .optional()
              .action((x, c) => c.copy(exclusions = x))
              .text("exclusions should be comma separated fields excluded from hash generation"),
            opt[String]("source-path")
              .required()
              .action((x, c) => c.copy(sourcePath = x))
              .text("source-path should point to new dataset directory"),
            opt[String]("table-type")
              .optional()
              .action((x, c) => c.copy(tableType = x))
              .text("table-type should be one of DIM or FACT"),
            opt[String]("table")
              .optional()
              .action((x, c) => c.copy(table = x))
              .text("table should be full path to a table e.g SCHEMA.TABLE"),
            opt[String]("partition-key")
              .optional()
              .action((x, c) => c.copy(partitionKey = x))
              .text("partition-key should be string value"),
            opt[String]("partition-value")
              .optional()
              .action((x, c) => c.copy(clPartitionValue = x))
              .text("partition-value key should be string value")
          )
      )
    }
    cmdLineParser
  }
}