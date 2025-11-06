package edu.ucr.cs.bdlab.dynoviz

import edu.ucr.cs.bdlab.beast.geolite.RasterMetadata
import org.apache.spark.internal.Logging

object RasterMetaDataGenerator extends Logging {
  /**
   * Generates an array of raster metadata objects for given zoom levels.
   *
   * @param zoomLevel The maximum zoom level for which metadata needs to be generated.
   * @return An array of raster metadata objects.
   */
  def generateRasterMetadata(zoomLevel: Int): Array[(RasterMetadata)] = {
    require(zoomLevel >= 0, "Zoom level must be non-negative")

    val srid = RasterConstants.WEBMERCATOR_SRID
    val tileWidth = RasterConstants.TILE_WIDTH
    val tileHeight = RasterConstants.TILE_HEIGHT
    val x1 = RasterConstants.ESPG3857_X1
    val y1 = RasterConstants.ESPG3857_Y1
    val x2 = RasterConstants.ESPG3857_X2
    val y2 = RasterConstants.ESPG3857_Y2

    val result = new Array[(RasterMetadata)](zoomLevel + 1)

    val minZoomLevel = 0
    for (level <- minZoomLevel to zoomLevel) {
      val scaledTileWidth = (tileWidth * math.pow(2, level)).toInt
      val scaledTileHeight = (tileHeight * math.pow(2, level)).toInt
      logDebug(s"Creating Raster MetaData for Zoom Level: $level")
      val metadata = RasterMetadata.create(x1, y1, x2, y2, srid, scaledTileWidth, scaledTileHeight, tileWidth, tileHeight)

        result(level) = (metadata)

    }
    result
  }

}
