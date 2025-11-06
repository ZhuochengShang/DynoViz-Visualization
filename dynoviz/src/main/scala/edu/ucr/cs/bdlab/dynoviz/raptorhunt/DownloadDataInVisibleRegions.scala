package edu.ucr.cs.bdlab.dynoviz.raptorhunt

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.dynoviz.RasterConstants
import edu.ucr.cs.bdlab.dynoviz.raptorhunt.IndexReader.{CsvRow, checkIfIndexExists, removeSpaces}
import edu.ucr.cs.bdlab.dynoviz.raptorhunt.RaptorUtils.{convertCoordinates, doOverlap}
import edu.ucr.cs.bdlab.raptor.GeoTiffReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.geotools.referencing.CRS
import org.locationtech.jts.geom.Coordinate

import java.awt.geom.Point2D
import java.net.URI
import scala.collection.mutable
import scala.io.Source

//TODO Fault Tolerance - While Read or write or processing, any exception should not break the flow of code
object DownloadDataInVisibleRegions extends Logging {
  def run(inputs: Array[String], sc: SparkContext): String = {
    val startTime = System.nanoTime()
    if (!validateExtentsInESPG3857(inputs)) {
      logInfo("Extents not valid")
      return null
    }
    val fs = FileSystem.get(new URI(inputs(4)), sc.hadoopConfiguration)

    val inputMBR= new Rectangle(inputs(0).toDouble, inputs(1).toDouble, inputs(2).toDouble, inputs(3).toDouble)

    if(checkIfIndexExists(inputs(4),fs))
    {
      logInfo("Index exists for the dataset, proceeding to use the index")
      val basePath = new Path(inputs(4))
      val fileStatuses = fs.listStatus(basePath)
      val files: mutable.HashSet[String] = mutable.HashSet()
      fileStatuses.filter(_.isFile).foreach { fileStatus =>
        if (fileStatus.getPath.getName.startsWith("_index") && fileStatus.getPath.getName.toLowerCase.endsWith(".csv")) {
          // Read the CSV file and process each row
          Source.fromInputStream(fs.open(fileStatus.getPath)).getLines().drop(1).foreach { line =>
            val row = line.split(",")
            logDebug(s"Valid Row: ${row.mkString(", ")}")
            val fileData: CsvRow = CsvRow(row(0), removeSpaces(row(1)).toInt, removeSpaces(row(2)).toDouble, removeSpaces(row(3)).toDouble, removeSpaces(row(4)).toDouble, removeSpaces(row(5)).toDouble, row(6))
            val sourceCRS = CRS.decode("EPSG:" + fileData.SRID)
            val targetCRS = CRS.decode("EPSG:3857")
            val transform = MathTransformCache.getTransform(sourceCRS, targetCRS)

            val fileCoordinatesTL = convertCoordinates(new Coordinate(fileData.x1, fileData.y1), transform)
            val fileCoordinatesBR = convertCoordinates(new Coordinate(fileData.x2, fileData.y2), transform)

            val fileMBR: Rectangle = new Rectangle(fileCoordinatesTL.x, fileCoordinatesTL.y, fileCoordinatesBR.x, fileCoordinatesBR.y)
            if (doOverlap(fileMBR, inputMBR)) {
              files += new Path(inputs(4), fileData.fileName).toString
            }
          }
        }
      }

      val delimiter = "||"
      val resultString = files.mkString(delimiter)

      return resultString
    }

    logInfo("Index does not exist for the dataset, proceeding to read the files")


    val files: mutable.HashSet[String] = processTiffFiles(inputMBR, inputs(4), fs)

    val endTime = System.nanoTime()
    val executionTime = (endTime - startTime) / 1000000

    logInfo(s"Execution Time : $executionTime ms")

    val delimiter = "||"
    val resultString = files.mkString(delimiter)

    resultString
  }

  private def processTiffFiles(inputMBR: Rectangle, directoryPath: String, fs: FileSystem): mutable.HashSet[String] = {
    val resultHashSet = mutable.HashSet[String]()
    try {
      val basePath = new Path(directoryPath)

      if (fs.exists(basePath)) {
        val fileStatuses = fs.listStatus(basePath)
        for (fileStatus <- fileStatuses) {
          if (fileStatus.isFile && fileStatus.getPath.getName.endsWith(".tif")) {
            val filePath = fileStatus.getPath.toString
            process(inputMBR, filePath) match {
              case Some(result) => resultHashSet += result
              case None => // Do nothing
            }
          }
        }
      } else {
        logInfo(s"Directory does not exist: $directoryPath")
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logError(s"Exception Occurred while processing TIFF files in given directory.")
    }
    resultHashSet
  }
  private def process(inputMBR: Rectangle, filePath: String): Option[String] = {
    val fileName = java.nio.file.Paths.get(filePath).getFileName.toString
    logInfo(s"Processing file: $fileName")
    val reader = new GeoTiffReader[Array[Int]]
    val rasterPath = new Path(filePath)
    reader.initialize(rasterPath.getFileSystem(new Configuration()), rasterPath.toString, "0", new BeastOptions())

    val topleft: Point2D.Double = new Point2D.Double()
    reader.metadata.gridToModel(reader.metadata.x1, reader.metadata.y1, topleft)

    val bottomRight: Point2D.Double = new Point2D.Double()
    reader.metadata.gridToModel(reader.metadata.x2, reader.metadata.y2, bottomRight)

    val sourceCRS = CRS.decode("EPSG:" + reader.metadata.srid.toString)
    val targetCRS = CRS.decode("EPSG:3857")

    val transform = MathTransformCache.getTransform(sourceCRS, targetCRS)

    val fileCoordinatesTL = convertCoordinates(new Coordinate(topleft.x, topleft.y), transform)
    val fileCoordinatesBR = convertCoordinates(new Coordinate(bottomRight.x, bottomRight.y), transform)

    val fileMBR: Rectangle = new Rectangle(fileCoordinatesTL.x, fileCoordinatesTL.y, fileCoordinatesBR.x, fileCoordinatesBR.y)

    if (doOverlap(fileMBR, inputMBR)) return Some(filePath)
    else logInfo(s"No overlap for File : ${fileMBR.toString} Input: ${inputMBR.toString}")
    None
  }

  /**
   * Validates whether the given array of inputs represents a rectangle within the bounds of EPSG:3857 (Web Mercator).
   *
   * @param inputs An array of strings representing the coordinates of a rectangle in the order: x1, y1, x2, y2.
   * @return `true` if the inputs form a valid rectangle within EPSG:3857 bounds, `false` otherwise.
   * @throws NumberFormatException if any of the input values cannot be parsed as doubles.
   */
  private def validateExtentsInESPG3857(inputs: Array[String]): Boolean = {
    if (inputs.length >= 4) {
      // Try to parse the first four values as doubles and check if it is in the bounds of EPSG3857
      try {
        val mbr: Rectangle = new Rectangle(inputs(0).toDouble, inputs(1).toDouble, inputs(2).toDouble, inputs(3).toDouble)
        val retVal: Boolean = RasterConstants.ESPG3857_X1 <= mbr.x1 && RasterConstants.ESPG3857_Y1 >= mbr.y1 && RasterConstants.ESPG3857_X2 >= mbr.x2 && RasterConstants.ESPG3857_Y2 <= mbr.y2
        retVal
      } catch {
        case _: NumberFormatException => false
      }
    } else {
      false
    }
  }
}
