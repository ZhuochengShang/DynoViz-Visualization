package edu.ucr.cs.bdlab.dynoviz.raptorhunt

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.dynoviz.raptorhunt.IndexReader.{CsvRow, checkIfIndexExists, removeSpaces}
import edu.ucr.cs.bdlab.dynoviz.raptorhunt.RaptorUtils.{calculateTileIDForPoint, convertCoordinates, isPointInRectangle}
import edu.ucr.cs.bdlab.raptor.GeoTiffReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.geotools.referencing.CRS
import org.locationtech.jts.geom.Coordinate

import java.awt.geom.Point2D
import java.net.URI
import scala.io.Source

object GetPointValue extends Logging {
  def run(opts: BeastOptions, inputs: Array[String], sc: SparkContext): String = {
    val startTime = System.nanoTime()

    if (!validateInput(inputs)) {
      logInfo("Input not valid")
      return "undefined"
    }
    val fs = FileSystem.get(new URI(inputs(0)), sc.hadoopConfiguration)
    val pointx: Double = inputs(1).toDouble
    val pointy: Double = inputs(2).toDouble
    val reader = new GeoTiffReader[Array[Int]]
    val sourceCRS = CRS.decode("EPSG:3857")

    logInfo(inputs(0))


    if(checkIfIndexExists(inputs(0),fs)) {
      logInfo("Index exists for the dataset, proceeding to use the index")
      val basePath = new Path(inputs(0))
      val fileStatuses = fs.listStatus(basePath)
      fileStatuses.filter(_.isFile).foreach { fileStatus =>
        if (fileStatus.getPath.getName.startsWith("_index") && fileStatus.getPath.getName.toLowerCase.endsWith(".csv")) {
          // Read the CSV file and process each row
          Source.fromInputStream(fs.open(fileStatus.getPath)).getLines().drop(1).foreach { line =>
            val row = line.split(",")
            logDebug(s"Valid Row: ${row.mkString(", ")}")
            val fileData: CsvRow = CsvRow(row(0), removeSpaces(row(1)).toInt, removeSpaces(row(2)).toDouble, removeSpaces(row(3)).toDouble, removeSpaces(row(4)).toDouble, removeSpaces(row(5)).toDouble, row(6))
            val targetCRS = CRS.decode("EPSG:" + fileData.SRID)
            val transform = MathTransformCache.getTransform(sourceCRS, targetCRS)
            val selectedPoint = convertCoordinates(new Coordinate(pointx, pointy), transform)
            val fileMBR: Rectangle = new Rectangle(fileData.x1, fileData.y1, fileData.x2, fileData.y2)
            if (isPointInRectangle(selectedPoint, fileMBR)) {
              logDebug(s"Overlap in the file ${fileData.fileName}")
              val rasterPath = new Path(basePath, fileData.fileName)
              reader.initialize(rasterPath.getFileSystem(new Configuration()), rasterPath.toString, "0", opts)
              val tileId = calculateTileIDForPoint(reader.metadata, selectedPoint.getX, selectedPoint.getY)
              logInfo(s"$tileId overlaps the point in the file ${fileData.fileName}")
              try {
                val itile = reader.readTile(tileId)
                if (!itile.isEmptyAt(selectedPoint.x, selectedPoint.y)) {
                  logInfo("Success: Tile Match at " + tileId + " at file " + fileData.fileName)
                  val endTime = System.nanoTime()
                  val executionTime = (endTime - startTime) / 1000000
                  logInfo(s"Execution Time : $executionTime ms")
                  return itile.getPointValue(selectedPoint.x, selectedPoint.y).mkString(", ")
                }
              } catch {
                case e: Exception =>
                  logError("Exception " + e.getMessage)
              }
            }
            else logInfo(s"No overlap for File : ${fileMBR.toString}")
            None
          }
        }
      }
    }

    logInfo("Index does not exist for the dataset, proceeding to read the files")
    val resultList: List[(String, Int)] = processTiffFiles(inputs(0), pointx, pointy, fs)
    for ((filePath, tile) <- resultList) {
      logInfo(s"Processing file: $filePath")
      val rasterPath = new Path(filePath)
      val fileName = rasterPath.getName
      reader.initialize(rasterPath.getFileSystem(sc.hadoopConfiguration), rasterPath.toString, "0", opts)
      val targetCRS = CRS.decode("EPSG:" + reader.metadata.srid.toString)
      val transform = MathTransformCache.getTransform(sourceCRS, targetCRS)
      val selectedPoint = convertCoordinates(new Coordinate(pointx, pointy), transform)
      try {
        val itile = reader.readTile(tile)
        if (!itile.isEmptyAt(selectedPoint.x, selectedPoint.y)) {
          logInfo("Success: Tile Match at " + tile + " at file " + fileName)
          val endTime = System.nanoTime()
          val executionTime = (endTime - startTime) / 1000000

          logInfo(s"Execution Time : $executionTime ms")
          return itile.getPointValue(selectedPoint.x, selectedPoint.y).mkString(", ")
        }
      } catch {
        case e: Exception =>
          logError("Exception " + e.getMessage)
      }
    }

    val endTime = System.nanoTime()
    val executionTime = (endTime - startTime) / 1000000

    logInfo(s"Execution Time : $executionTime ms")
    "undefined"
  }

  private def processTiffFiles(directoryPath: String, pointX: Double, pointY: Double, fs: FileSystem): List[(String, Int)] = {
    var resultList: List[(String, Int)] = Nil
    try {
      val basePath = new Path(directoryPath)
      if (fs.exists(basePath)) {
        val fileStatuses = fs.listStatus(basePath)
        for (fileStatus <- fileStatuses) {
          if (fileStatus.isFile && fileStatus.getPath.getName.endsWith(".tif")) {
            val filePath = fileStatus.getPath.toString
            val processedResult: Option[(String, Int)] = process(filePath, pointX, pointY)
            processedResult.foreach(resultTuple => resultList :+= resultTuple)
            }
          }
        }
      else {
        logInfo(s"Directory does not exist: $directoryPath")
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logError(s"Exception Occurred while processing TIFF files in given directory.")
    }
    resultList
  }
  private def process(filePath: String, pointX: Double, pointY: Double): Option[(String, Int)] = {
    val reader = new GeoTiffReader[Array[Int]]
    val rasterPath = new Path(filePath)
    val fileName = rasterPath.getName
    logInfo(s"Processing file: $fileName")
    reader.initialize(rasterPath.getFileSystem(new Configuration()), rasterPath.toString, "0", new BeastOptions())

    val topleft: Point2D.Double = new Point2D.Double()
    reader.metadata.gridToModel(reader.metadata.x1, reader.metadata.y1, topleft)

    val bottomRight: Point2D.Double = new Point2D.Double()
    reader.metadata.gridToModel(reader.metadata.x2, reader.metadata.y2, bottomRight)

    //CRS Conversion
    val sourceCRS = CRS.decode("EPSG:3857")
    val targetCRS = CRS.decode("EPSG:" + reader.metadata.srid.toString)
    val transform = MathTransformCache.getTransform(sourceCRS, targetCRS)

    val fileMBR: Rectangle = new Rectangle(topleft.x, topleft.y, bottomRight.x, bottomRight.y)
    val selectedPoint = convertCoordinates(new Coordinate(pointX, pointY), transform)

    if (isPointInRectangle(selectedPoint, fileMBR)) {
      logInfo(s"Overlap in the file $fileName")
      val tileId = calculateTileIDForPoint(reader.metadata, selectedPoint.getX, selectedPoint.getY)
      logInfo(s"$tileId overlaps the point in the file $fileName")
      return Some(filePath, tileId)
    }
    else logInfo(s"No overlap for File : ${fileMBR.toString}")
    None
  }

  private def validateInput(inputs: Array[String]): Boolean = {
    try {
      inputs(1).toDouble
      inputs(2).toDouble
      true
    } catch {
      case _: NumberFormatException => false
    }
  }
}
