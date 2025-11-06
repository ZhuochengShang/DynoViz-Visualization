package edu.ucr.cs.bdlab.dynoviz

import edu.ucr.cs.bdlab.beast.geolite.{ RasterMetadata}
import edu.ucr.cs.bdlab.dynoviz.raptorhunt.CalculateDensityValue
import org.apache.spark.internal.Logging

object RasterMetaDataGeneratorFilter extends Logging {
  /**
   * Generates an array of raster metadata objects for given zoom levels.
   *
   * @param zoomLevel The maximum zoom level for which metadata needs to be generated.
   * @return An array of raster metadata objects.
   */
  def generateRasterMetadata(zoomLevel: Int, densityValue: Int, inputMetadataList: (Int,Int,Double,Double)): Array[(RasterMetadata,Int)] = {
    require(zoomLevel >= 0, "Zoom level must be non-negative")

    val inputRasterWidth = inputMetadataList._1
    val inputRasterHeight = inputMetadataList._2
    val inputRasterExtentW = inputMetadataList._3
    val inputRasterExtentH = inputMetadataList._4

    val srid = RasterConstants.WEBMERCATOR_SRID
    val tileWidth = RasterConstants.TILE_WIDTH
    val tileHeight = RasterConstants.TILE_HEIGHT
    val x1 = RasterConstants.ESPG3857_X1
    val y1 = RasterConstants.ESPG3857_Y1
    val x2 = RasterConstants.ESPG3857_X2
    val y2 = RasterConstants.ESPG3857_Y2

    val result = new Array[(RasterMetadata,Int)](zoomLevel + 1)

    val minZoomLevel = 0
    val densityThreshold = densityValue

    for (level <- minZoomLevel to zoomLevel) {
      val scaledRasterWidth = (tileWidth * math.pow(2, level)).toLong
      val scaledRasterHeight = (tileHeight * math.pow(2, level)).toLong
      val currentDensity = CalculateDensityValue.calculateDensityValue(inputRasterWidth,inputRasterHeight,inputRasterExtentW,inputRasterExtentH,level)
      if (currentDensity > densityThreshold) {
        logDebug(s"Creating Raster MetaData for Zoom Level: $level")
        val metadata = RasterMetadata.create(x1, y1, x2, y2, srid, scaledRasterWidth.toInt, scaledRasterHeight.toInt, tileWidth, tileHeight)
        result(level) = (metadata, level)
      }
      logInfo(s"Current Density is : $currentDensity for Zoom Level: $level")
    }
    result.filter(_!=null)
  }


}
