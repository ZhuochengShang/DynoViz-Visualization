/**
 * This object, `IndexBuilder`, is responsible for building and saving an index for GeoTIFF files in a given directory.
 *
 * The index includes file names, spatial reference IDs (SRID), and bounding box information for each GeoTIFF file.
 *
 * @note This code assumes a specific structure for the GeoTIFF files and extracts metadata such as SRID and bounding box.
 * @example
 * {{{
 *   val path = "file:///Users/johnwesley/Downloads/LandsatTiff/LS2"
 *   val fs = FileSystem.get(new URI(path), new Configuration())
 *   val index = IndexBuilder.buildIndex(fs, path)
 *   IndexBuilder.saveIndexToFile(index, path)
 * }}}
 * @see Rectangle Case class representing a rectangle with top-left and bottom-right corners.
 */
package edu.ucr.cs.bdlab.dynoviz

import edu.ucr.cs.bdlab.beast.common.{BeastOptions, CLIOperation}
import edu.ucr.cs.bdlab.beast.util.{OperationMetadata, Parallel2}
import edu.ucr.cs.bdlab.dynoviz.raptorhunt.MathTransformCache
import edu.ucr.cs.bdlab.raptor.{GeoTiffReader, RasterFileRDD}
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.conf
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.beast.CRSServer
import org.apache.spark.internal.Logging
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.CRS
import org.locationtech.jts.geom.Envelope
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.operation.MathTransform

import java.awt.geom.Point2D
import java.io.{BufferedWriter, File, FileWriter, OutputStreamWriter}
import java.net.{URI, URLEncoder}
import java.nio.file.FileVisitResult
import scala.collection.mutable.{Map => MutableMap}


@OperationMetadata(
  shortName =  "buildrasterindexp",
  description = "Builds the index for a directory with raster files. The index includes file names, spatial reference IDs (SRID), and bounding box information for each GeoTIFF file",
  inputArity = "1",
  outputArity = "0"
)
object IndexBuilderParallel extends CLIOperation with Logging {
  /**
   * Entry point for the IndexBuilder application. This method is intended to be used as part of a Spark application
   * and implements the `run` method of the Beast framework.
   *
   * @param opts    The BeastOptions containing configuration options for the application.
   * @param inputs  An array of input paths representing HDFS or Local File System's URI of the path.
   * @param outputs An array of output paths (not used in the current implementation).
   * @param sc      The SparkContext for the Spark application.
   * @return The result of the application.
   */
  override def run(opts: BeastOptions, inputs: Array[String], outputs: Array[String], sc: SparkContext): Any = {
    val startTime = System.nanoTime()

    if (inputs.length != 1) {
      logInfo("Usage: buildrasterindex <hdfs_or_local_path_to_dir>")
      System.exit(1)
    }
    val path = inputs(0)

    // Use the Hadoop FileSystem API to get the appropriate FileSystem instance
    val fs = FileSystem.get(new URI(path), new Configuration())
    //val fs = new Path(path).getFileSystem(new Configuration())
    val index = buildIndex(sc, fs, path)

    // Save the index to a file
    saveIndexToFile(index, path)
    val endTime = System.nanoTime()
    val executionTime = (endTime - startTime) / 1000000
    logInfo(s"Execution Time : $executionTime ms")
  }

  /**
   * Builds an index for GeoTIFF files in the specified directory.
   *
   * @param fs       The Hadoop FileSystem instance.
   * @param basePath The base path of the directory containing GeoTIFF files.
   * @return A map containing file names as keys and tuple values of SRID and bounding box information.
   */
  private def buildIndex(sparkContext: SparkContext, fs: FileSystem, basePath: String): Map[String, (Int, String)] = {
    try {
      // Get raster files and parallelize them
      val statusArray: Array[FileStatus] = getRasterFiles(fs, new Path(basePath))
      val statusRDD = sparkContext.parallelize(statusArray, statusArray.length)

      // Process files in parallel and return a key-value pair for each file
      val indexRDD = statusRDD.map { fileStatus =>
        val fileNamePath = fileStatus.getPath.toString
        val fileName = fileStatus.getPath.getName.toString
        val reader = new GeoTiffReader[Array[Int]]
        val rasterPath = new Path(fileNamePath)

        try {
          reader.initialize(rasterPath.getFileSystem(new Configuration()), rasterPath.toString, "0", new BeastOptions())

          val tl = new Point2D.Double()
          reader.metadata.gridToModel(reader.metadata.x1, reader.metadata.y1, tl)
          val br = new Point2D.Double()
          reader.metadata.gridToModel(reader.metadata.x2, reader.metadata.y2, br)
          val mbr = Rectangle(tl.x, tl.y, br.x, br.y)

          if (reader.metadata.srid < 0) {
            logInfo(s"An unexpected SRID error occurred: $fileName")
            val enve = reader.metadata.extents
            val sourceCRS: CoordinateReferenceSystem = CRSServer.sridToCRS(enve.getSRID)
            val res = generateWKTin4326(reader.metadata.envelope, sourceCRS)
            val combinedString = res._2 + ",\"" + res._1 + "\""
            (fileName, (4326, combinedString))
          } else {
            val wktString = generateWKTin4326(mbr, CRS.decode("EPSG:" + reader.metadata.srid.toString))
            val combinedString = mbr.toString + ",\"" + wktString + "\""
            (fileName, (reader.metadata.srid, combinedString))
          }

        } finally {
          reader.close() // Ensure reader is closed
        }
      }

      // Collect results from all partitions to the driver and convert to a Map
      indexRDD.collect().toMap

    } catch {
      case e: Exception =>
        logInfo(s"An unexpected error occurred: ${e.getMessage}")
        Map.empty[String, (Int, String)] // Return an empty map if there's an exception
    }
  }

  /**
   * Saves the index to a CSV file in the specified directory. Can work for both local file system and hdfs.
   *
   * @param index         The index map containing file names, SRID, and bounding box information.
   * @param directoryPath The directory path where the index CSV file will be saved.
   */
  private def saveIndexToFile(index: Map[String, (Int, String)], directoryPath: String): Unit = {
    val outputPath = directoryPath + "/_index.csv"
    val writer =
      directoryPath match {
        case local: String if local.startsWith("file://") =>
          val uri = new URI(outputPath)
          val pathWithoutScheme = new File(uri).getPath
          val file = new File(pathWithoutScheme)
          file.createNewFile()
          new BufferedWriter(new FileWriter(file))

        case _ =>
          val fs = FileSystem.get(new URI(outputPath), new Configuration())
          val op = new Path(outputPath)
          val outputStream = fs.create(op)
          new BufferedWriter(new OutputStreamWriter(outputStream))
      }

    try {
      writer.write(s"FileName, SRID, x1, y1, x2, y2, WKT\n")
      index.foreach { case (fileName, (value1, value2)) =>
        writer.write(s"$fileName,$value1,$value2\n")
      }
    } finally {
      writer.close()
    }
  }

  /**
   * Generates a Well-Known Text (WKT) representation of a bounding box (rectangle) in EPSG:4326.
   *
   * @param rectangle   The rectangle with coordinates (x1, y1) as the top-left corner and (x2, y2) as the bottom-right corner.
   * @param sourceCRS   The Coordinate Reference System (CRS) of the input rectangle.
   * @return            A WKT string representing the bounding box in EPSG:4326.
   */
  private def generateWKTin4326(rectangle: Rectangle, sourceCRS: CoordinateReferenceSystem): String = {
    // Create a bounding box (Envelope)
    val envelope = new Envelope(rectangle.x1, rectangle.x2, rectangle.y1, rectangle.y2)

    // Transform the bounding box to EPSG:4326
    val transform: MathTransform = MathTransformCache.getTransform(sourceCRS, CRS.decode("EPSG:4326"))
    val transformedEnvelope = JTS.transform(envelope, transform)

    // Create a WKT string for the bounding box in EPSG:4326
    val wktString = s"POLYGON ((${transformedEnvelope.getMinY} ${transformedEnvelope.getMinX}, " +
      s"${transformedEnvelope.getMaxY} ${transformedEnvelope.getMinX}, " +
      s"${transformedEnvelope.getMaxY} ${transformedEnvelope.getMaxX}, " +
      s"${transformedEnvelope.getMinY} ${transformedEnvelope.getMaxX}, " +
      s"${transformedEnvelope.getMinY} ${transformedEnvelope.getMinX}))"

    wktString
  }

  private def generateWKTin4326(rectangle: Envelope, sourceCRS: CoordinateReferenceSystem): (String, String) = {
    // Create a bounding box (Envelope)
    val envelope = rectangle //new Envelope(rectangle.x1, rectangle.x2, rectangle.y1, rectangle.y2)

    // Transform the bounding box to EPSG:4326
    val transform: MathTransform = MathTransformCache.getTransform(sourceCRS, CRS.decode("EPSG:4326"))
    val transformedEnvelope = JTS.transform(envelope, transform)

    // Create a WKT string for the bounding box in EPSG:4326

    val x1 = transformedEnvelope.getMinX
    val mbr = Rectangle(transformedEnvelope.getMinX, transformedEnvelope.getMaxY, transformedEnvelope.getMaxX, transformedEnvelope.getMinY)

    // Create a WKT string for the bounding box in EPSG:4326
    val wktString = s"POLYGON ((${transformedEnvelope.getMinY} ${transformedEnvelope.getMinX}, " +
      s"${transformedEnvelope.getMaxY} ${transformedEnvelope.getMinX}, " +
      s"${transformedEnvelope.getMaxY} ${transformedEnvelope.getMaxX}, " +
      s"${transformedEnvelope.getMinY} ${transformedEnvelope.getMaxX}, " +
      s"${transformedEnvelope.getMinY} ${transformedEnvelope.getMinX}))"

    (wktString, mbr.toString)
  }

  private def getRasterFiles(rasterFileSystem: FileSystem, rasterPath: Path): Array[FileStatus] = {
    if (rasterFileSystem.getFileStatus(rasterPath).isDirectory) {
      rasterFileSystem.listStatus(rasterPath, new PathFilter() {
        override def accept(path: Path): Boolean =
          path.getName.toLowerCase().endsWith(".tif") || path.getName.toLowerCase().endsWith(".hdf")
      })
    } else {
      Array(rasterFileSystem.getFileStatus(rasterPath))
    }
  }
  /**
   * Case class representing a rectangle with top-left corner (x1, y1) and bottom-right corner (x2, y2).
   *
   * @param x1 The x-coordinate of the top-left corner.
   * @param y1 The y-coordinate of the top-left corner.
   * @param x2 The x-coordinate of the bottom-right corner.
   * @param y2 The y-coordinate of the bottom-right corner.
   */
  case class Rectangle(x1: Double, y1: Double, x2: Double, y2: Double) {
    /**
     * Returns a string representation of the rectangle.
     *
     * @return A string representing the rectangle in the format "Rectangle(x1, y1, x2, y2)".
     */
    override def toString: String = f"$x1,$y1,$x2,$y2"
  }
}
