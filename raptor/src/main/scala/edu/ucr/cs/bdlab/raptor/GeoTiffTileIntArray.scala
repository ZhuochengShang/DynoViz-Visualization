package edu.ucr.cs.bdlab.raptor

import com.esotericsoftware.kryo.DefaultSerializer
import edu.ucr.cs.bdlab.beast.geolite.{RasterFeature, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.tiff.AbstractTiffTile
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DataType, IntegerType}

/**
 * A special class for GeoTiffTiles where each pixel has two or more bands of integer type.
 * @param tileID the ID of the tile within the tile grid
 * @param tiffTile the underlying tiff tile
 * @param fillValue the fill value in the tiff tile that marks empty pixels
 * @param rasterMetadata the metadata of the raster file
 */
@DefaultSerializer(classOf[GeoTiffTileSerializer[Any]])
class GeoTiffTileIntArray(tileID: Int, tiffTile: AbstractTiffTile,
                          fillValue: Int, pixelDefined: Array[Byte], rasterMetadata: RasterMetadata, rasterFeatures: RasterFeature)
  extends AbstractGeoTiffTile[Array[Int]](tileID, tiffTile, fillValue, pixelDefined, rasterMetadata, rasterFeatures) {

  override def getPixelValue(i: Int, j: Int): Array[Int] = {
    val values = new Array[Int](numComponents)
    for (b <- values.indices)
      values(b) = tiffTile.getSampleValueAsInt(i, j, b)
    values
  }

  override def componentType: DataType = IntegerType

  override def copy(): Row = ???

}
