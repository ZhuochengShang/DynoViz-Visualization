package edu.ucr.cs.bdlab.dynoviz

import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.RasterRDD
import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.geolite.RasterMetadata
import edu.ucr.cs.bdlab.beast.util.{OperationMetadata, OperationParam}
import edu.ucr.cs.bdlab.raptor.RaptorMixin.RasterReadMixinFunctions
import edu.ucr.cs.bdlab.raptor.RasterOperationsFocal
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.{ArrayType, DataType, IntegerType}

import java.io.OutputStreamWriter


@OperationMetadata(
  shortName = "mplotraster",
  description = "Plots the input tif file as a multilevel pyramid image",
  inputArity = "1",
  outputArity = "1"
)
object RasterMultiLevelPlot extends CLIOperation with Logging {

  /**
   * Zoom Level upto which we generate the MultiLevel Imaging of Input
   */
  @OperationParam(description = "Zoom Level", defaultValue = "0")
  val ZOOM_LEVEL = "zoom"

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
    val raster: RasterRDD[Array[Int]] = sc.geoTiff[Array[Int]](inputs(0))
    val outputPathStr: String = outputs(0)
    var zoomLevel: Int = opts.getInt(ZOOM_LEVEL, 0)
    if (zoomLevel < 0) {
      logWarning("Invalid zoom level provided. Zoom level must be non-negative. Defaulting to 0")
      zoomLevel = 0
    }

    validateDataType(raster.first().pixelType)
    val metadataList: Array[(RasterMetadata)] = RasterMetaDataGenerator.generateRasterMetadata(zoomLevel)//.map(_._1)

    metadataList.zipWithIndex.foreach { case (metadata, index) =>
      logInfo(s"Generating Raster Layer for level $index")
      val reshaped: RasterRDD[Array[Int]] = RasterOperationsFocal.reshapeNN(raster, _=>metadata)
      reshaped.foreach(tile => try {
        PNGUtils.generatePNG(tile, metadata, index, outputPathStr, opts)
        //NPYUtils.tileToNpyBytes(tile, metadata, index, outputPathStr, opts)
      } catch {
        case e: Exception =>
          logError("Error occurred during reshaping raster data:", e)
      }
      )
      val endTime = System.nanoTime()
      val executionTime = (endTime - startTime) / 1000000
      logInfo(s"Execution Time : $executionTime ms")
    }

    val zoomLevel0_metadata = metadataList(0)
    val zoomLevel0_Xcenter = ( zoomLevel0_metadata.x2 - zoomLevel0_metadata.x1 ) / 2
    val zoomLevel0_Ycenter = ( zoomLevel0_metadata.y2 - zoomLevel0_metadata.y1 ) / 2

    writeIndexHTMLFile(opts,inputs(0), outputPathStr)
    //writeIndexHTMLnpyFile(opts,inputs(0), outputPathStr,zoomLevel0_Xcenter,zoomLevel0_Ycenter)
    // todo: change the center of img when creating html js

    val endTime = System.nanoTime()
    val executionTime = (endTime - startTime) / 1000000
    logInfo(s"Execution Time : $executionTime ms")
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
    } catch {
      case e: Exception =>
        logError("Error occurred while writing HTML file:", e)
    } finally {
      writer.close()
    }
  }

  /**
   * An exception indicating that the provided data type is not supported.
   *
   * @param message The message describing the reason for the unsupported data type.
   */
  private case class UnsupportedDataTypeException(message: String) extends Exception(message)
}

