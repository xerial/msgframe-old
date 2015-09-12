/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package xerial.msgframe.core

import java.io.ByteArrayOutputStream
import java.sql.{Struct, Blob, Clob, ResultSet, Ref}

import org.msgpack.core.{MessagePacker, MessageUnpacker, MessagePack, MessageFormat}
import org.msgpack.value.ValueType


object MsgFrame {

  import java.sql.{Types=>sql}

  trait ColMapper {
    def pack(rs:ResultSet, colIndex:Int, packer:MessagePacker) : Unit
  }
  object StringColMapper extends ColMapper {
    override def pack(rs: ResultSet, colIndex: Int, packer: MessagePacker) = packer.packString(rs.getString(colIndex))
  }
  object BigDecimalColMapper extends ColMapper {
    override def pack(rs: ResultSet, colIndex: Int, packer: MessagePacker) = packer.packBigInteger(rs.getBigDecimal(colIndex).toBigInteger)
  }
  object BooleanColMapper extends ColMapper {
    override def pack(rs: ResultSet, colIndex: Int, packer: MessagePacker) = packer.packBoolean(rs.getBoolean(colIndex))
  }
  object ByteColMapper extends ColMapper {
    override def pack(rs: ResultSet, colIndex: Int, packer: MessagePacker) = packer.packByte(rs.getByte(colIndex))
  }
  object ShortColMapper extends ColMapper {
    override def pack(rs: ResultSet, colIndex: Int, packer: MessagePacker) = packer.packShort(rs.getShort(colIndex))
  }
  object IntColMapper extends ColMapper {
    override def pack(rs: ResultSet, colIndex: Int, packer: MessagePacker) = packer.packInt(rs.getInt(colIndex))
  }
  object LongColMapper extends ColMapper {
    override def pack(rs: ResultSet, colIndex: Int, packer: MessagePacker) = packer.packLong(rs.getLong(colIndex))
  }
  object FloatColMapper extends ColMapper {
    override def pack(rs: ResultSet, colIndex: Int, packer: MessagePacker) = packer.packFloat(rs.getFloat(colIndex))
  }
  object DoubleColMapper extends ColMapper {
    override def pack(rs: ResultSet, colIndex: Int, packer: MessagePacker) = packer.packDouble(rs.getDouble(colIndex))
  }
  object ByteArrayColMapper extends ColMapper {
    override def pack(rs: ResultSet, colIndex: Int, packer: MessagePacker) = {
      val b = rs.getBytes(colIndex)
      packer.packBinaryHeader(b.length)
      packer.writePayload(b)
    }
  }
  object DateColMapper extends ColMapper {
    override def pack(rs: ResultSet, colIndex: Int, packer: MessagePacker) = packer.packLong(rs.getDate(colIndex).getTime)
  }
  object TimeColMapper extends ColMapper {
    override def pack(rs: ResultSet, colIndex: Int, packer: MessagePacker) = packer.packLong(rs.getTime(colIndex).getTime)
  }
  object TimeStampColMapper extends ColMapper {
    override def pack(rs: ResultSet, colIndex: Int, packer: MessagePacker) = {
      val ts = rs.getTimestamp(colIndex)
      packer.packLong(ts.getTime + (ts.getNanos / 1000000)) // limited accuracy within 1 milliseconds
    }
  }
  object UrlColMapper extends ColMapper {
    override def pack(rs: ResultSet, colIndex: Int, packer: MessagePacker) = packer.packString(rs.getURL(colIndex).toExternalForm)
  }
  object NullColMapper extends ColMapper {
    override def pack(rs: ResultSet, colIndex: Int, packer: MessagePacker) = packer.packNil()
  }

  def fromSQL(rs: ResultSet) = {
    val metadata = rs.getMetaData
    val numColumns = metadata.getColumnCount
    val colNames = (1 to numColumns).map(i => metadata.getColumnName(i))
    val colTypes = (1 to numColumns).map(i => metadata.getColumnType(i))

    val colMapper : Seq[ColMapper] = (colTypes.map { colType =>
      colType match {
        case sql.CHAR | sql.VARCHAR | sql.LONGVARCHAR => StringColMapper
        case sql.NUMERIC | sql.DECIMAL => BigDecimalColMapper
        case sql.BIT | sql.BOOLEAN => BooleanColMapper
        case sql.TINYINT => ByteColMapper
        case sql.SMALLINT => ShortColMapper
        case sql.INTEGER => IntColMapper
        case sql.BIGINT => LongColMapper
        case sql.REAL => FloatColMapper
        case sql.FLOAT | sql.DOUBLE => DoubleColMapper
        case sql.BINARY | sql.VARBINARY | sql.LONGVARBINARY => ByteArrayColMapper
        case sql.DATE => DateColMapper
        case sql.TIME => TimeColMapper
        case sql.TIMESTAMP => TimeStampColMapper
        case sql.CLOB => ByteArrayColMapper
        case sql.BLOB => ByteArrayColMapper
        case sql.DATALINK => UrlColMapper
        case sql.ARRAY => NullColMapper
        case sql.DISTINCT => NullColMapper
        case sql.STRUCT => NullColMapper
        case sql.REF => NullColMapper
        case sql.JAVA_OBJECT => NullColMapper
      }
    }).toIndexedSeq

    val rows = Seq.newBuilder[Any]
    val buf = new ByteArrayOutputStream()
    val packer = MessagePack.newDefaultPacker(buf)
    val rowIndexes = Seq.newBuilder[Long]
    while(rs.next()) {
      rowIndexes += packer.getTotalWrittenBytes
      packer.packArrayHeader(numColumns)
      for(i <- (0 until numColumns)) {
        colMapper(i).pack(rs, i+1, packer)
      }
    }
    packer.close()

    new RawOrientedFrame(colNames, colTypes, buf.toByteArray, rowIndexes.result().toArray)
  }

  val jdbcToJavaType = Map(
    sql.CHAR -> classOf[String],
    sql.VARCHAR -> classOf[String],
    sql.LONGVARCHAR -> classOf[String],
    sql.NUMERIC -> classOf[java.math.BigDecimal],
    sql.DECIMAL -> classOf[java.math.BigDecimal],
    sql.BIT -> classOf[Boolean],
    sql.BOOLEAN -> classOf[Boolean],
    sql.TINYINT -> classOf[Byte],
    sql.SMALLINT -> classOf[Short],
    sql.INTEGER -> classOf[Int],
    sql.BIGINT -> classOf[Long],
    sql.REAL -> classOf[Float],
    sql.FLOAT -> classOf[Double],
    sql.DOUBLE -> classOf[Double],
    sql.BINARY -> classOf[Array[Byte]],
    sql.VARBINARY -> classOf[Array[Byte]],
    sql.LONGVARBINARY -> classOf[Array[Byte]],
    sql.DATE -> classOf[java.sql.Date],
    sql.TIME -> classOf[java.sql.Time],
    sql.TIMESTAMP -> classOf[java.sql.Timestamp],
    sql.CLOB -> classOf[Clob],
    sql.BLOB -> classOf[Blob],
    sql.ARRAY -> classOf[Array],
    sql.DISTINCT -> classOf[AnyRef],
    sql.STRUCT -> classOf[Struct],
    sql.REF -> classOf[Ref],
    sql.DATALINK -> classOf[java.net.URL],
    sql.JAVA_OBJECT -> classOf[AnyRef]
  )
}

/**
 *
 */
trait MsgFrame {
  def cols: Seq[String]
  def colTypes: Seq[Class[_]]

}

class RawOrientedFrame(val cols:Seq[String], val colTypes:Seq[Class[_]], val data:Array[Byte], val rowOffsets:Array[Long])
  extends MsgFrame {

}


