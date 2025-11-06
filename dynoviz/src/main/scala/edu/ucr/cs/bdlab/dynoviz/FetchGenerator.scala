package edu.ucr.cs.bdlab.dynoviz

import edu.ucr.cs.bdlab.beast.cg.Reprojector.{TransformationInfo, findTransformationInfo}
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import edu.ucr.cs.bdlab.beast.util.OperationMetadata
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.spark.SparkContext
import org.apache.spark.beast.CRSServer
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType}
import org.geotools.referencing.CRS

import java.io.OutputStreamWriter

@OperationMetadata(
  shortName = "fetch",
  description = "Plots the input tif file as a multilevel pyramid image",
  inputArity = "1",
  outputArity = "0"
)
object FetchGenerator extends CLIOperation with Logging {

  /**
   * Zoom Level upto which we generate the MultiLevel Imaging of Input
   */
  /**
   * Run the main function using the given user command-line options and spark context
   *
   * @param opts user options for configuring the operation
   * @param sc   the Spark context used to run the operation
   * @return an optional result of this operation
   */
  override def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Any = {
    val startTime = System.nanoTime()

    // run rasterindex
    //val raster: RasterRDD[Array[Int]] = sc.geoTiff[Array[Int]](inputs(0))
    FetchImage.writeALL(inputs(0))
    val endTime = System.nanoTime()
    val executionTime = (endTime - startTime) / 1000000
    val executionTimeS = (endTime - startTime) /1e9
    logInfo(s"Execution Time : $executionTime ms")
    logInfo(s"Execution Time : $executionTimeS s")
  }

  /**
   * Validates whether the provided DataType is an ArrayType of IntegerType.
   *
   * @param dataType The DataType to be validated.
   * @throws UnsupportedDataTypeException if the DataType is not an ArrayType of IntegerType.
   */
  private def validateDataType(dataType: DataType): Unit = {
    val isArrayOfIntegers: Boolean = dataType match {
      case ArrayType(IntegerType, _) => true
      case _ => false
    }
    if (!isArrayOfIntegers) {
      val errorMessage = "The DataType is not an ArrayType of IntegerType. This type is currently not supported."
      logError(errorMessage)
      throw UnsupportedDataTypeException(errorMessage)
    }
  }

  /**
   * Writes the index HTML file for the plotted raster data.
   *
   * @param opts    User options for configuring the operation.
   * @param datasetPath Input Dataset path
   * @param outputPathStr Output path
   * @param outputs Array of output file paths.
   */
  private def writeIndexHTMLFile(opts: BeastOptions, datasetPath: String, outputPathStr: String): Unit = {
    val indexHTML: String = HTMLGenerator.getIndexHTMLFile(opts, datasetPath, outputPathStr)
    val outFS = new Path(outputPathStr).getFileSystem(opts.loadIntoHadoopConf())
    val outputPath = new Path(outputPathStr, "index.html")
    val outputStream: FSDataOutputStream = outFS.create(outputPath)
    val writer = new OutputStreamWriter(outputStream, "UTF-8")
    try {
      logDebug(s"Attempting to write HTML File to: ${outputPath.toString} in ${outFS.toString}")
      writer.write(indexHTML)
      writer.close()
    } catch {
      case e: Exception =>
        logError("Error occurred while writing HTML file:", e)
    } finally {
      writer.close()
    }
  }

  private def writeIndexHTMLnpyFile(opts: BeastOptions, datasetPath: String, outputPathStr: String, centerX: Float, centerY: Float): Unit = {
    val indexHTML: String = HTMLnpyGenerator.getIndexHTMLFile(opts, datasetPath, outputPathStr, centerX, centerY)
    val outFS = new Path(outputPathStr).getFileSystem(opts.loadIntoHadoopConf())
    val outputPath = new Path(outputPathStr, "index.html")
    val outputStream: FSDataOutputStream = outFS.create(outputPath)
    val writer = new OutputStreamWriter(outputStream, "UTF-8")
    try {
      logDebug(s"Attempting to write HTML File to: ${outputPath.toString} in ${outFS.toString}")
      writer.write(indexHTML)
      writer.close()
      outputStream.close()
    } catch {
      case e: Exception =>
        logError("Error occurred while writing HTML file:", e)
    } finally {
      writer.close()
      outputStream.close()
    }
  }

  def calculateXY(zoomlevel: Int): (Int,Int) = {
    var currX2 = 0
    var currY2 = 0
    for (z <- 1 to zoomlevel){
      currX2 = currX2 + Math.pow(2,z-1).toInt
      currY2 = currY2 + Math.pow(2,z-1).toInt
    }
    (currX2, currY2)
  }

  def getInputMetadataWH(raster:RDD[ITile[Array[Int]]]): (Int,Int) = {
    val targetCRS = CRS.decode("EPSG:3857") //4326, 3857
    val allMetadata: Array[RasterMetadata] = RasterMetadata.allMetadata(raster)

    var minX1 = Double.MaxValue
    var minY1 = Double.MaxValue

    var maxX2 = Double.MinValue
    var maxY2 = Double.MinValue

    var minCellsizeX = Double.MaxValue
    var minCellsizeY = Double.MaxValue

    var rasterWidth = 0
    var rasterHeight = 0
    allMetadata.foreach(originMetadata => {
      val sourceCRS = CRSServer.sridToCRS(originMetadata.srid)

      val x1 = originMetadata.x1
      val y1 = originMetadata.y1
      val x2 = originMetadata.x2
      val y2 = originMetadata.y2
      val corners = Array[Double](x1, y1,
        x2, y1,
        x2, y2,
        x1, y2)
      originMetadata.g2m.transform(corners, 0, corners, 0, 4)

      val transform: TransformationInfo = findTransformationInfo(sourceCRS, targetCRS)
      transform.mathTransform.transform(corners, 0, corners, 0, 4)

      minX1 = minX1 min corners(0) min corners(2) min corners(4) min corners(6)
      maxX2 = maxX2 max corners(0) max corners(2) max corners(4) max corners(6)
      minY1 = minY1 min corners(1) min corners(3) min corners(5) min corners(7)
      maxY2 = maxY2 max corners(1) max corners(3) max corners(5) max corners(7)

      val col = originMetadata.rasterWidth
      val row = originMetadata.rasterHeight

      minCellsizeX = minCellsizeX min ((maxX2 - minX1).abs / col)
      minCellsizeY = minCellsizeY min ((maxY2 - minY1).abs / row)

    })

    rasterWidth = Math.floor(Math.abs(maxX2 - minX1) / minCellsizeX).toInt
    rasterHeight = Math.floor(Math.abs(maxY2 - minY1) / minCellsizeY).toInt

    //val targetMetadata = RasterMetadata.create(minX1, maxY2, maxX2, minY1, 4326, rasterWidth, rasterHeight, initialMetadata.tileWidth, initialMetadata.tileHeight)
    (rasterWidth,rasterHeight)
  }

  /**
   * An exception indicating that the provided data type is not supported.
   *
   * @param message The message describing the reason for the unsupported data type.
   */
  private case class UnsupportedDataTypeException(message: String) extends Exception(message)
}

