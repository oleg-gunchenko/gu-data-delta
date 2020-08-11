package ru.dwh.etl.delta

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import org.apache.commons.codec.binary.Hex
import org.apache.commons.codec.digest.{DigestUtils, MurmurHash3}
import org.apache.commons.lang3.RandomStringUtils
import org.apache.spark.sql.Row

import scala.collection.mutable.{ArrayBuffer, ListBuffer}

trait DCFieldFormat {
  def getValue(row: Row, name: String): Any = {
    val idx = row.fieldIndex(name)
    val rr = row.get(idx)
    val isSimple = !rr.isInstanceOf[Row]
    return if (isSimple) rr else {
      val f: Row = rr.asInstanceOf[Row]
      val fv = f.get(f.fieldIndex("value"))
      fv
    }
  }

  def fromDC2Plain(row: Row): Row = {
    var data: ArrayBuffer[Any] = ArrayBuffer()
    row.schema.fields.foreach(field => data += getValue(row, field.name))
    Row.fromSeq(data)
  }
}

class UpdateRowTransformer(keys: Set[String], exclusions: Set[String]) extends DCFieldFormat {
  var excl: Set[String] = exclusions ++ keys

  def process(row: Row): Row = {
    val result = ListBuffer[String]()
    row.schema.fields.foreach(field => {
      val name = field.name
      val fv = getValue(row, name)
      val nv = if (this.excl.contains(name))
        fv.asInstanceOf[String]
      else
        RandomStringUtils.random(20)
      result += nv
    })
    Row.fromSeq(result)
  }
}

object Transformations {
  def apply(config: Config, keys: Set[String], exclusions: Set[String]): HashTransformations = {
    val kBufferSize = config.keyBufferSize * 1024
    val vBufferSize = config.valBufferSize * 1024

    config.hashClass match {
      case "murmur" => new MurmurHashTransformations(keys, exclusions, config.inline, kBufferSize, vBufferSize)
      case "md5" => new Md5HashTransformations(keys, exclusions, config.inline, kBufferSize, vBufferSize)
      case "sha" => new ShaHashTransformations(keys, exclusions, config.inline, kBufferSize, vBufferSize)
    }
  }

  abstract class HashTransformations(keys: Set[String], exclusions: Set[String], inline: Boolean,
                                     kbuffer: ByteBuffer, vbuffer: ByteBuffer) extends DCFieldFormat {
    val SEPARATOR: Byte = ';'.toByte

    def this(keys: Set[String], exclusions: Set[String], inline: Boolean,
             kBufferSize: Int, vBufferSize: Int) = {
      this(keys, exclusions, inline, ByteBuffer.allocate(kBufferSize), ByteBuffer.allocate(vBufferSize))

      kbuffer.mark()
      vbuffer.mark()
    }

    def hashBuffers(kbuffer: ByteBuffer, vbuffer: ByteBuffer): (String, String) = ???

    def addHashes(row: Row): Row = {
      val result: ArrayBuffer[Any] = ArrayBuffer[Any]()
      row.schema.fields.foreach(field => {
        val name = field.name
        val fv = getValue(row, name)

        if (!inline && keys.contains(name))
          result += fv

        if (!exclusions.contains(name)) {
          val buffer = if (keys.contains(name)) kbuffer else vbuffer
          if (fv != null) {
            val tc = fv.getClass.getSimpleName
            tc match {
              case "Boolean" => buffer.put(fv.asInstanceOf[Boolean].toString.getBytes(StandardCharsets.UTF_8))
              case "String" => buffer.put(fv.asInstanceOf[String].getBytes(StandardCharsets.UTF_8))
              case "Integer" => buffer.putInt(fv.asInstanceOf[Integer])
              case "Long" => buffer.putLong(fv.asInstanceOf[Long])
              case "Double" => buffer.putDouble(fv.asInstanceOf[Double])
              case "Float" => buffer.putFloat(fv.asInstanceOf[Float])
              case "Short" => buffer.putShort(fv.asInstanceOf[Short])
              case "Byte" => buffer.putShort(fv.asInstanceOf[Byte])
            }
          }
          buffer.put(SEPARATOR)
        }
      })

      val (keyHash, valHash) = hashBuffers(kbuffer, vbuffer)

      vbuffer.reset()
      kbuffer.reset()

      Row.fromSeq(Seq(keyHash, valHash) ++ (if (inline) row.toSeq else result))
    }
  }

  private class MurmurHashTransformations(keys: Set[String], exclusions: Set[String], inline: Boolean, kBufferSize: Int, vBufferSize: Int)
    extends HashTransformations(keys, exclusions, inline, kBufferSize, vBufferSize) {
    override def hashBuffers(kbuffer: ByteBuffer, vbuffer: ByteBuffer): (String, String) = {
      val keyHash = Hex.encodeHexString(MurmurHash3.hash32x86(kbuffer.array()).toString.getBytes(StandardCharsets.UTF_8))
      val valHash = Hex.encodeHexString(MurmurHash3.hash32x86(vbuffer.array()).toString.getBytes(StandardCharsets.UTF_8))
      (keyHash, valHash)
    }
  }

  private class Md5HashTransformations(keys: Set[String], exclusions: Set[String], inline: Boolean, kBufferSize: Int, vBufferSize: Int)
    extends HashTransformations(keys, exclusions, inline, kBufferSize, vBufferSize) {
    override def hashBuffers(kbuffer: ByteBuffer, vbuffer: ByteBuffer): (String, String) = {
      val keyHash = DigestUtils.md5Hex(kbuffer.array())
      val valHash = DigestUtils.md5Hex(vbuffer.array())
      (keyHash, valHash)
    }
  }

  private class ShaHashTransformations(keys: Set[String], exclusions: Set[String], inline: Boolean, kBufferSize: Int, vBufferSize: Int)
    extends HashTransformations(keys, exclusions, inline, kBufferSize, vBufferSize) {
    override def hashBuffers(kbuffer: ByteBuffer, vbuffer: ByteBuffer): (String, String) = {
      val keyHash = DigestUtils.sha512_256Hex(kbuffer.array())
      val valHash = DigestUtils.sha512_256Hex(vbuffer.array())
      (keyHash, valHash)
    }
  }

}
