package edu.ucr.cs.bdlab.dynoviz.raptorhunt

import edu.ucr.cs.bdlab.beast.cg.Reprojector.{TransformationInfo, findTransformationInfo}
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.RasterRDD
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.util.{OperationMetadata, OperationParam}
import edu.ucr.cs.bdlab.dynoviz.RasterConstants
import edu.ucr.cs.bdlab.raptor.RaptorMixin.RasterReadMixinFunctions
import org.apache.spark.SparkContext
import org.apache.spark.beast.CRSServer
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType}
import org.geotools.referencing.CRS


object CalculateDensityValue  {
  def calculateDensityValue(inputRasterWidth: Int, inputRasterHeight: Int, inputRasterExtentW: Double, inputRasterExtentH: Double, level: Int): Double = {
    val tileWidth = RasterConstants.TILE_WIDTH
    val tileHeight = RasterConstants.TILE_HEIGHT
    val x1 = RasterConstants.ESPG3857_X1
    val y1 = RasterConstants.ESPG3857_Y1
    val x2 = RasterConstants.ESPG3857_X2
    val y2 = RasterConstants.ESPG3857_Y2

    val scaledRasterWidth = (tileWidth * math.pow(2, level)).toLong
    val scaledRasterHeight = (tileHeight * math.pow(2, level)).toLong

    val inputPixel: Long = inputRasterWidth.toLong * inputRasterHeight.toLong

    val cellSizeX = Math.abs(x2 - x1) / scaledRasterWidth.toInt
    val cellSizeY = Math.abs(y2 - y1) / scaledRasterHeight.toInt

    val coverageW = inputRasterExtentW / cellSizeX
    val coverageH = inputRasterExtentH / cellSizeY

    val coveragePercentage = (coverageW * coverageH) ///(scaledRasterWidth*scaledRasterHeight)

    val currentDensity = inputPixel / (256 * 256) / coveragePercentage / math.pow(4, level)
    currentDensity

  }


  /**
   * An exception indicating that the provided data type is not supported.
   *
   * @param message The message describing the reason for the unsupported data type.
   */
  private case class UnsupportedDataTypeException(message: String) extends Exception(message)
}


