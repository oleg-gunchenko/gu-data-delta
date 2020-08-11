package ru.dwh.etl.delta

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes.createStructField
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.apache.spark.sql.{Column, DataFrame}


class TableMeta(config: Config, fields: Seq[StructField]) {
  /*
    kvhash - множество из двух хэшей для ключей и значимых атрибутов
    ids - множество атрибутов, составляющих первичный ключ
    m_part_keys - множество атрибутов, партиционирующих метатаблицу
    s_part_keys - множество атрибутов, партиционирующих таблицу исходных данных
    hashex - множество атрибутов таблицы данных (холостых атрибутов), исключенных из подсчета хэша значений атрибутов данных
    payload - множество атрибутов таблицы данных
    payload_bare - множество атрибутов таблицы данных за исключением ключей
    payload_hashex - множество атрибутов таблицы данных за исключением холостых атрибутов
    payload_hashex_ids - множество атрибутов таблицы данных за исключением холостых атрибутов и атрибутов первичного ключа

    mirror_meta - множество атрибутов, составляющих метаатрибуты зеркала (для фактов и размерностей)
    delta_meta - множество атрибутов, составляющих метаатрибуты дельты (для фактов и размерностей)
  */
  def kv_hash(): Seq[StructField] = Seq(
    TableMetaFactory.META_FIELDS.getOrElse(TableMetaFactory.META_KEY_FIELD, null),
    TableMetaFactory.META_FIELDS.getOrElse(TableMetaFactory.META_VALUE_FIELD, null)
  )

  private def filterFields(fields: Set[String]): Seq[StructField] = {
    val result = this.fields.filter(f => fields.contains(f.name.toLowerCase))
    result
  }

  def filterNotFields(fields: Set[String]): Seq[StructField] = {
    val result = this.fields.filter(f => !fields.contains(f.name.toLowerCase))
    result
  }

  def ids(): Seq[StructField] = filterFields(config.getKeySet())

  def s_part_keys(): Seq[StructField] = filterFields(config.getPartitionKeySet())

  def m_part_keys(): Seq[StructField] = Seq(TableMetaFactory.META_FIELDS.getOrElse(TableMetaFactory.DWSJOB, null))

  def hash_ex(): Seq[StructField] = filterFields(TableMetaFactory.META_FIELDS.keySet ++ config.getExclusionSet())

  def payload(): Seq[StructField] = fields

  def payload_bare(): Seq[StructField] = filterFields((payload().toSet -- ids().toSet).map(f => f.name))

  def payload_hashex(): Seq[StructField] = filterFields((payload().toSet -- hash_ex().toSet).map(f => f.name))

  def hash_payload(): Seq[StructField] = if (config.inline) kv_hash() ++ payload() else kv_hash() ++ ids()

  def meta_table(): Seq[StructField] = if (config.inline)
    meta() ++ kv_hash() ++ payload()
  else
    meta() ++ kv_hash() ++ ids() ++ s_part_keys()

  def payload_metaex(): Seq[StructField] = filterNotFields(meta().map(_.name).toSet)

  def meta(): Seq[StructField] = Assembler(config).meta()
}


object TableMetaFactory {
  val META_KEY_FIELD: String = "DWSKEYHASH".toUpperCase()
  val META_VALUE_FIELD: String = "DWSVALHASH".toUpperCase()
  val DWSJOB: String = "DWSJOB".toUpperCase()
  val NK: String = "NK".toUpperCase()
  val DWSACT: String = "dwsact".toUpperCase()
  val DWSUNIACT: String = "dwsuniact".toUpperCase()
  val DWSAUTO: String = "dwsauto".toUpperCase()
  val DWSARCHIVE: String = "dwsarchive".toUpperCase()

  val META_FIELDS: Map[String, StructField] = Map(
    META_KEY_FIELD -> createStructField(META_KEY_FIELD, DataTypes.StringType, false),
    META_VALUE_FIELD -> createStructField(META_VALUE_FIELD, DataTypes.StringType, false),
    DWSJOB -> createStructField(DWSJOB, DataTypes.StringType, false),
    NK -> createStructField(NK, DataTypes.StringType, false),
    DWSACT -> createStructField(DWSACT, DataTypes.StringType, true),
    DWSUNIACT -> createStructField(DWSUNIACT, DataTypes.StringType, true),
    DWSAUTO -> createStructField(DWSAUTO, DataTypes.StringType, true),
    DWSARCHIVE -> createStructField(DWSARCHIVE, DataTypes.StringType, true)
  )

  def columns(fields: Seq[StructField]): Seq[Column] = fields.map(f => col(f.name))

  def apply(config: Config, df: DataFrame): TableMeta = {
    apply("__src__", "__mir__", config, df)
  }

  def apply(suffixNew: String, suffixOld: String, config: Config, df: DataFrame): TableMeta = {
    val fields = df.schema.fields

    new TableMeta(config, df.schema.fields)
  }
}
