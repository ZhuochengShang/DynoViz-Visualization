package edu.ucr.cs.bdlab.beast.geolite
import com.esotericsoftware.kryo.DefaultSerializer
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.{StructField, StructType}
@DefaultSerializer(classOf[RasterFeatureSerializer])
class RasterFeature(values: Array[Any], schema: StructType) extends GenericRowWithSchema(values, schema) with Serializable {

  private[beast] def getValues: Array[Any] = values

  val fileName: String = {
    try {
      val iFileName = schema.fieldIndex("fileName")
      values(iFileName).toString
    } catch {
      case _: IllegalArgumentException => null
    }
  }

  val fileDatetime: java.sql.Timestamp = {
    try {
      val iFileDatetime = schema.fieldIndex("fileDatetime")
      values(iFileDatetime).asInstanceOf[java.sql.Timestamp]
    } catch {
      case _: IllegalArgumentException => null
    }
  }

}

object RasterFeature {
  def create(names: Array[String], values: Array[Any]): RasterFeature  =
    new RasterFeature(values, RasterSchemaHelper.inferSchema(names, values))

  def createEmpty(): RasterFeature =
    new RasterFeature(Array(""), RasterSchemaHelper.inferSchema(Array(""), Array("")))

  def append(rasterFeature: RasterFeature, name: String, value: Any): RasterFeature  = {
    val values: Seq[Any] = Row.unapplySeq(rasterFeature).get :+ value
    val schema: Seq[StructField] = rasterFeature.schema :+ StructField(name, RasterSchemaHelper.detectType(value))
    new RasterFeature(values.toArray, StructType(schema))
  }

}