package edu.ucr.cs.bdlab.beast.sql

import edu.ucr.cs.bdlab.beast.geolite.RasterMetadata
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

import java.awt.geom.AffineTransform

class RasterMetadataType extends UserDefinedType[RasterMetadata] {
// extend UDT

  override def sqlType: StructType = StructType(Seq(StructField("x1", IntegerType), StructField("y1", IntegerType),
    StructField("x2", IntegerType), StructField("y2", IntegerType),
    StructField("tileWidth", IntegerType), StructField("tileHeight", IntegerType),
    StructField("srid", IntegerType), StructField("g2m", ArrayType(DoubleType))))

  override def serialize(obj: RasterMetadata): Any = RasterMetadataType.toRow(obj)


  override def deserialize(datum: Any): RasterMetadata = {
    datum match {
      case x: InternalRow => RasterMetadataType.fromRow(x)
    }
  }

  override def userClass: Class[RasterMetadata] = classOf[RasterMetadata]
}

case object RasterMetadataType extends RasterMetadataType {

  def fromRow(x: InternalRow): RasterMetadata =
    new RasterMetadata(
      x.getInt(0), x.getInt(1), x.getInt(2), x.getInt(3),
      x.getInt(4), x.getInt(5), x.getInt(6), new AffineTransform(x.getArray(7).asInstanceOf[Array[Double]])
    )

  def toRow(rasterMetadata: RasterMetadata): InternalRow = InternalRow.apply(
    rasterMetadata.x1, rasterMetadata.y1, rasterMetadata.x2, rasterMetadata.y2,
    rasterMetadata.tileWidth, rasterMetadata.tileHeight, rasterMetadata.srid,
    rasterMetadata.g2m
  )


}
