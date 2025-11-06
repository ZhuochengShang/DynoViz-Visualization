/*
 * Copyright 2024 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucr.cs.bdlab.beast.geolite

import com.esotericsoftware.kryo.{Kryo, KryoSerializable}
import com.esotericsoftware.kryo.io.{Input, Output}
import edu.ucr.cs.bdlab.beast.geolite.Feature.{ordinalTypes, readType, readValue, writeType, writeValue}
import edu.ucr.cs.bdlab.beast.util.{BitArray, KryoInputToObjectInput, KryoOutputToObjectOutput}
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.Encoders.kryo
import org.apache.spark.sql.Row
import org.apache.spark.sql.Row.empty.isNullAt
import org.apache.spark.sql.types.{ArrayType, BinaryType, BooleanType, ByteType, DataType, DataTypes, DateType, DecimalType, DoubleType, FloatType, IntegerType, LongType, MapType, NullType, ShortType, StringType, StructField, StructType, TimestampType}
import org.locationtech.jts.geom.Geometry

import java.io.{ByteArrayInputStream, Externalizable, InputStream, ObjectInput, ObjectInputStream, ObjectOutput, ObjectOutputStream}

object RasterSchemaHelper extends Externalizable with KryoSerializable {

  def writeExternalObj(schema:StructType, out: ObjectOutput): Unit = {
    // Number of attributes
    out.writeShort(schema.length)
    if (schema.length > 0) {
      // Attribute names
      for (field <- schema)
        out.writeUTF(if (field.name == null) "" else field.name)
      // Attribute types
      for (field <- schema)
        writeType(field.dataType, out)
    }
  }
  def readExternalObj(in: ObjectInput): StructType = {
    // Read number of attributes
    val recordLength: Int = in.readShort()
    val attributeNames = new Array[String](recordLength)
    val attributeTypes = new Array[DataType](recordLength)
    // Read attribute names
    for (i <- 0 until recordLength)
      attributeNames(i) = in.readUTF()
    // Read attribute types
    for (i <- 0 until recordLength)
      attributeTypes(i) = readType(in)
    val _schema = StructType((0 until recordLength).map(i => StructField(attributeNames(i), attributeTypes(i))))
    _schema
  }


  /**
   * Read a data type from an input stream that was written with the function [[writeType()]]
   *
   * @param in the input stream to read from
   * @return the created data type
   */
  def readType(in: ObjectInput): DataType = {
    val typeOrdinal = in.readByte()
    if (typeOrdinal == 11) {
      // Indicates a map type
      val keyType: DataType = readType(in)
      val valueType: DataType = readType(in)
      MapType(keyType, valueType, valueContainsNull = true)
    } else if (typeOrdinal == 12) {
      // Indicates an array type
      val elementType: DataType = readType(in)
      ArrayType(elementType, containsNull = true)
    } else {
      ordinalTypes.getOrElse(typeOrdinal, BinaryType)
    }
  }

  def write(schema:StructType,kryo: Kryo, out: Output): Unit = writeExternalObj(schema,new KryoOutputToObjectOutput(kryo,out))

  def readIn(kryo:Kryo,in: Input): StructType = readExternalObj(new KryoInputToObjectInput(kryo,in))

  override def writeExternal(out: ObjectOutput): Unit = ???

  override def readExternal(in: ObjectInput): Unit = ???

  override def write(kryo: Kryo, output: Output): Unit = ???

  override def read(kryo: Kryo, input: Input): Unit = ???

  /**
   * Infer schema from the values. If a value is `null`, the type is inferred as [[BinaryType]]
   * @param values the array of values
   * @return
   */
  def inferSchema(names: Array[String], values: Array[Any]): StructType = StructType(values.zipWithIndex.map(vi => StructField(names(vi._2), detectType(vi._1))))

  /**
   * Detect the data type for the given value.
   * @param value A value to detect its type
   * @return a detected data type for the given value.
   */
  def detectType(value: Any): DataType = value match {
    case null => BinaryType
    case _: Byte => ByteType
    case _: Short => ShortType
    case _: Int => IntegerType
    case _: Long => LongType
    case _: Float => FloatType
    case _: Double => DoubleType
    case _: String => StringType
    case x: java.math.BigDecimal => DecimalType(x.precision(), x.scale())
    case _: java.sql.Date | _: java.time.LocalDate => DateType
    case _: java.sql.Timestamp | _: java.time.Instant => TimestampType
    case _: Array[Byte] => BinaryType
    case r: Row => r.schema
    case _: Geometry => GeometryDataType
    case m: Map[Any, Any] =>
      val keyType: DataType = if (m.isEmpty) StringType else detectType(m.head._1)
      val valueType: DataType = if (m.isEmpty) StringType else detectType(m.head._2)
      MapType(keyType, valueType, valueContainsNull = true)
    case _ => BinaryType
  }
}
