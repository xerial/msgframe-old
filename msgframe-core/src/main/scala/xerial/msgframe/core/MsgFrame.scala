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
import java.sql.ResultSet

import org.joda.time.DateTime
import org.msgpack.core.buffer.{MessageBufferInput, MessageBuffer}
import org.msgpack.core.{MessageFormat, MessageUnpacker, MessagePack, MessagePacker}
import org.msgpack.value.{ValueType, ImmutableValue, Variable, Value}
import xerial.core.log.Logger


object MsgFrame extends Logger {

  import java.sql.{Types => sql}

  trait ColMapper {
    def pack(rs: ResultSet, colIndex: Int, packer: MessagePacker): Unit
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
    override def pack(rs: ResultSet, colIndex: Int, packer: MessagePacker) = {
      val dateString = rs.getString(colIndex)
      val dt = DateTime.parse(dateString)
      packer.packLong(dt.getMillis)
    }
  }

  object TimeColMapper extends ColMapper {
    override def pack(rs: ResultSet, colIndex: Int, packer: MessagePacker) = packer.packLong(rs.getTime(colIndex).getTime)
  }

  object TimeStampColMapper extends ColMapper {
    override def pack(rs: ResultSet, colIndex: Int, packer: MessagePacker) = {
      // TODO use -1 (Timestamp extension type)
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
    if(rs == null) {
      new RowOrientedFrame(Seq.empty, Seq.empty, Array.empty[Byte], Array.empty[Int])
    }
    else {
      val metadata = rs.getMetaData
      val numColumns = metadata.getColumnCount
      val colNames = (1 to numColumns).map(i => metadata.getColumnName(i))
      val colTypes = (1 to numColumns).map(i => metadata.getColumnType(i))
      val msgTypes: Seq[MessageType] = colTypes.map(jdbcToMessageType.get(_).getOrElse(MessageType.STRING)).toIndexedSeq

      val colMapper: Seq[ColMapper] = (colTypes.map { colType =>
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
          case _ =>
            warn(s"unknown type: ${colType}")
            NullColMapper
        }
      }).toIndexedSeq

      val rows = Seq.newBuilder[Any]
      val buf = new ByteArrayOutputStream()
      val rowIndexes = Seq.newBuilder[Int]

      withResource(MessagePack.newDefaultPacker(buf)) { packer =>
        while (rs.next()) {
          rowIndexes += packer.getTotalWrittenBytes.toInt
          packer.packArrayHeader(numColumns)
          for (i <- (0 until numColumns)) {
            colMapper(i).pack(rs, i + 1, packer)
          }
        }
      }
      new RowOrientedFrame(colNames, msgTypes, buf.toByteArray, rowIndexes.result().toArray)
    }
  }

  sealed trait MessageType {
    def canReadFrom(valueType:ValueType) : Boolean
  }


  object MessageType {
    object STRING extends MessageType {
      def canReadFrom(valueType:ValueType) : Boolean = {
        valueType match {
          case ValueType.NIL => false
          case ValueType.BOOLEAN => true
          case ValueType.INTEGER => true
          case ValueType.FLOAT => true
          case ValueType.STRING => true
          case ValueType.BINARY => true
          case ValueType.ARRAY => true // as json
          case ValueType.MAP => true // as json
          case ValueType.EXTENSION => true // as json
        }
      }
    }
    object LONG extends MessageType {
      override def canReadFrom(valueType: ValueType): Boolean = {
        valueType match {
          case ValueType.NIL => false
          case ValueType.BOOLEAN => true
          case ValueType.INTEGER => true
          case ValueType.FLOAT => true
          case ValueType.STRING => true
          case ValueType.BINARY => false
          case ValueType.ARRAY => false
          case ValueType.MAP => false
          case ValueType.EXTENSION => false
        }
      }
    }
    object DOUBLE extends MessageType {
      override def canReadFrom(valueType: ValueType): Boolean = {
        valueType match {
          case ValueType.NIL => false
          case ValueType.BOOLEAN => true
          case ValueType.INTEGER => true
          case ValueType.FLOAT => true
          case ValueType.STRING => true
          case ValueType.BINARY => false
          case ValueType.ARRAY => false
          case ValueType.MAP => false
          case ValueType.EXTENSION => false
        }
      }
    }
    object BOOLEAN extends MessageType {
      override def canReadFrom(valueType: ValueType): Boolean = {
        valueType match {
          case ValueType.NIL => false
          case ValueType.BOOLEAN => true
          case ValueType.INTEGER => true
          case ValueType.FLOAT => true
          case ValueType.STRING => true
          case ValueType.BINARY => false
          case ValueType.ARRAY => false
          case ValueType.MAP => false
          case ValueType.EXTENSION => false
        }
      }
    }
    object BYTE_ARRAY extends MessageType {
      override def canReadFrom(valueType: ValueType): Boolean = ???
    }
    object TIME extends MessageType {
      override def canReadFrom(valueType: ValueType): Boolean = ???
    }
    object TIMESTAMP extends MessageType {
      override def canReadFrom(valueType: ValueType): Boolean = ???
    }
    object UNSUPPORTED extends MessageType{
      override def canReadFrom(valueType: ValueType): Boolean = ???
    }
  }

  import MessageType._

  val jdbcToMessageType: Map[Int, MessageType] = Map(
    sql.CHAR -> STRING,
    sql.VARCHAR -> STRING,
    sql.LONGVARCHAR -> STRING,
    sql.NUMERIC -> LONG,
    sql.DECIMAL -> LONG,
    sql.BIT -> BOOLEAN,
    sql.BOOLEAN -> BOOLEAN,
    sql.TINYINT -> LONG,
    sql.SMALLINT -> LONG,
    sql.INTEGER -> LONG,
    sql.BIGINT -> LONG,
    sql.REAL -> DOUBLE,
    sql.FLOAT -> DOUBLE,
    sql.DOUBLE -> DOUBLE,
    sql.BINARY -> BYTE_ARRAY,
    sql.VARBINARY -> BYTE_ARRAY,
    sql.LONGVARBINARY -> BYTE_ARRAY,
    sql.DATE -> TIME,
    sql.TIME -> TIME,
    sql.TIMESTAMP -> TIMESTAMP,
    sql.CLOB -> STRING,
    sql.BLOB -> BYTE_ARRAY,
    sql.ARRAY -> UNSUPPORTED,
    sql.DISTINCT -> UNSUPPORTED,
    sql.STRUCT -> UNSUPPORTED,
    sql.REF -> UNSUPPORTED,
    sql.DATALINK -> STRING,
    sql.JAVA_OBJECT -> UNSUPPORTED
  )
}

import xerial.msgframe.core.MsgFrame._

/**
 *
 */
trait MsgFrame {
  def colNames: Seq[String]
  def colTypes: Seq[MessageType]

  def numRows : Int
  def numColumns : Int
}

class RowOrientedFrame(val colNames: Seq[String], val colTypes: Seq[MessageType], data: Array[Byte], rowOffsets: Array[Int])
  extends MsgFrame {



  def numRows = rowOffsets.length
  def numColumns = colNames.length

  def colType(col:Int) : MessageType = colTypes(col)
  def apply(row:Int, col:Int) : Value = ???


  private def unpackerAt(row:Int, col:Int): MessageUnpacker = {
    // TODO avoid creating new unpacker instance by preparing thread-safe unpacker
    val unpacker = MessagePack.newDefaultUnpacker(data, rowOffsets(row), rowByteArraySize(row))
    var i = 0
    while(i < col && unpacker.hasNext) {
      unpacker.skipValue()
      i += 1
    }
    unpacker
  }

  private def rowByteArraySize(row:Int) = {
    val limit = if(row + 1 >= rowOffsets.length) data.length else rowOffsets(row+1)
    limit - rowOffsets(row)
  }

  def isNil(row:Int, col:Int, readType:MessageType) = {
    val unpacker = unpackerAt(row, col)
    val vt = unpacker.getNextFormat.getValueType
    !readType.canReadFrom(vt)
  }

  def readValue(row:Int, col:Int, holder:Variable) : Value = unpackerAt(row, col).unpackValue(holder)
  def getValue(row:Int, col:Int) : ImmutableValue = unpackerAt(row, col).unpackValue
  def getLong(row:Int, col:Int) : Long = {
    val unpacker = unpackerAt(row, col)
    val mf = unpacker.getNextFormat
    ???
  }

  def getString(row:Int, col:Int) : String = {
    unpackerAt(row, col).unpackString
  }
  def getBoolean(row:Int, col:Int) : Boolean = {
    unpackerAt(row, col).unpackBoolean
  }
  def getBytes(row:Int, col:Int) : MessageBuffer = {
    val unpacker = unpackerAt(row, col)
    val vt = unpacker.getNextFormat.getValueType
    vt match {
      case ValueType.NIL => MessageBuffer.newBuffer(0)
      case ValueType.BOOLEAN =>
        val b = unpacker.unpackBoolean()
        MessageBuffer.wrap(Array[Byte](if(b) 1 else 0))
      case ValueType.INTEGER => ???
      case ValueType.FLOAT => ???
      case ValueType.STRING => ???
      case ValueType.BINARY => ???
      case ValueType.ARRAY => ???
      case ValueType.MAP => ???
      case ValueType.EXTENSION => ???
    }
  }
  def getTime(row:Int, col:Int) : DateTime = ???
  def getTimestamp(row:Int, col:Int) : DateTime = ???

  override def toString() = {
    val s = new StringBuilder
    for(row <- 0 until numRows) {
      if(row > 0) {
        s.append("\n")
      }
      val begin = rowOffsets(row)
      val end = if(row < numRows - 1) rowOffsets(row+1) else data.length
      val unpacker = MessagePack.newDefaultUnpacker(data, begin, end - begin)
      val numCols = unpacker.unpackArrayHeader()
      val cols = for(col <- 0 until numCols) yield {
        val v = unpacker.unpackValue()
        v.toString
      }
      s.append(cols.mkString("\t"))
    }
    s.result()
  }
}




