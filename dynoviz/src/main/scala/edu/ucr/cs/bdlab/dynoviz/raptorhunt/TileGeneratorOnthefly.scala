package edu.ucr.cs.bdlab.dynoviz.raptorhunt

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.RasterMetadata
import edu.ucr.cs.bdlab.dynoviz.RasterConstants
import edu.ucr.cs.bdlab.dynoviz.raptorhunt.IndexReader.{CsvRow, removeSpaces}
import edu.ucr.cs.bdlab.dynoviz.raptorhunt.RaptorUtils.{calculateSpatialExtent, concatenateTransforms, convertCoordinates, doOverlap, getTileIDsInRectangle, validateXYBoundsForZoomLevel}
import edu.ucr.cs.bdlab.raptor.GeoTiffReader
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.geotools.geometry.DirectPosition2D
import org.geotools.referencing.CRS
import org.geotools.referencing.operation.transform.AffineTransform2D
import org.locationtech.jts.geom.Coordinate
import org.opengis.geometry.DirectPosition
import org.opengis.referencing.operation.MathTransform

import java.awt.Color
import java.awt.geom.Point2D
import java.awt.image.BufferedImage
import java.net.URI
import scala.io.Source

object TileGeneratorOnthefly extends Logging {

  var imageCount = 0
  def run(source: String, tileDir: String, indexFile:FileStatus, z: Int, currX: Int, currY: Int, sc: SparkContext): (BufferedImage,Int) = {
    val sourceData = source
    val zoomLevel = z
    val x = currX
    val y = currY

    val dir = tileDir //tile dir
    val fs = FileSystem.get(new URI(dir), sc.hadoopConfiguration)
    val reader = new GeoTiffReader[Array[Int]]

    var image = new BufferedImage(256, 256, BufferedImage.TYPE_INT_ARGB)
    for {
      y <- 0 until 256
      x <- 0 until 256
    } {
      val argb = 0x00FFFFFF // Fully transparent
      image.setRGB(x, y, argb)
    }

    validateXYBoundsForZoomLevel(zoomLevel, x, y)
    val inputMBR: Rectangle = calculateSpatialExtent(zoomLevel, x, y) // in epsg 3857
    val tgtMetadata = RasterMetadata.create(x1 = inputMBR.x1, y1 = inputMBR.y1, x2 = inputMBR.x2, y2 = inputMBR.y2, srid = 3857,
      rasterWidth = RasterConstants.TILE_WIDTH, rasterHeight = RasterConstants.TILE_HEIGHT, tileWidth = RasterConstants.TILE_WIDTH, tileHeight = RasterConstants.TILE_HEIGHT)

    val targetCRS = CRS.decode("EPSG:3857")
    val targetCRS4326 = CRS.decode("EPSG:4326")

    val transform = MathTransformCache.getTransform(targetCRS, targetCRS4326)
    val revTransform = MathTransformCache.getTransform(targetCRS4326, targetCRS)

    var pixelCount: Int = 0
      logInfo("Index exists for the dataset, proceeding to use the index")
      val basePath = new Path(sourceData)
      // go through index file
      Source.fromInputStream(fs.open(indexFile.getPath)).getLines().drop(1).foreach { line =>
        val row = line.split(",")
        logDebug(s"Valid Row: ${row.mkString(", ")}")
        val fileData: CsvRow = CsvRow(row(0), removeSpaces(row(1)).toInt, removeSpaces(row(2)).toDouble, removeSpaces(row(3)).toDouble, removeSpaces(row(4)).toDouble, removeSpaces(row(5)).toDouble, row(6))
        val rasterPath = new Path(basePath, fileData.fileName)
        val sourceCRS = CRS.decode("EPSG:" + fileData.SRID)

        if (fileData.SRID == 4326) {
          val transformedInputTL = convertCoordinates(new Coordinate(inputMBR.x1, inputMBR.y1), transform)
          val transformedInputBR = convertCoordinates(new Coordinate(inputMBR.x2, inputMBR.y2), transform)
          val inputMBR4326: Rectangle = new Rectangle(transformedInputTL.x, transformedInputTL.y, transformedInputBR.x, transformedInputBR.y) //4326MBR
          val tgtMetadata4326 = RasterMetadata.create(x1 = inputMBR4326.x1, y1 = inputMBR4326.y1, x2 = inputMBR4326.x2, y2 = inputMBR4326.y2, srid = 3857,
            rasterWidth = RasterConstants.TILE_WIDTH, rasterHeight = RasterConstants.TILE_HEIGHT, tileWidth = RasterConstants.TILE_WIDTH, tileHeight = RasterConstants.TILE_HEIGHT)
          val fileMBR: Rectangle = new Rectangle(fileData.x1, fileData.y1, fileData.x2, fileData.y2)
          if (doOverlap(inputMBR4326, fileMBR)) {
            val res = convertProcess(reader, rasterPath, transform, revTransform, fileData.x1, fileData.y1, fileData.x2, fileData.y2, inputMBR4326, tgtMetadata4326, image, pixelCount)
            image = res._1
            pixelCount = pixelCount + res._2
            logInfo("Overlapped image 4326")
          }
          reader.close()
        } else {
          val transform = MathTransformCache.getTransform(sourceCRS, targetCRS)
          val revTransform = MathTransformCache.getTransform(targetCRS, sourceCRS)

          val fileCoordinatesTL = convertCoordinates(new Coordinate(fileData.x1, fileData.y1), transform)
          val fileCoordinatesBR = convertCoordinates(new Coordinate(fileData.x2, fileData.y2), transform)

          val fileMBR: Rectangle = new Rectangle(fileCoordinatesTL.x, fileCoordinatesTL.y, fileCoordinatesBR.x, fileCoordinatesBR.y)
          if (doOverlap(inputMBR, fileMBR)) {
            val res = convertProcess(reader, rasterPath, transform, revTransform, fileData.x1, fileData.y1, fileData.x2, fileData.y2, inputMBR, tgtMetadata, image, pixelCount)
            image = res._1
            pixelCount = pixelCount + res._2
          }
          reader.close()
        }
        reader.close()
      }
      fs.close()
    (image, pixelCount)
  }

  def convertProcess(reader: GeoTiffReader[Array[Int]], rasterPath: Path,
                     transform: MathTransform, revTransform: MathTransform,
                     X1: Double, Y1: Double, X2: Double, Y2: Double,
                     inputMBR: Rectangle, tgtMetadata: RasterMetadata, image: BufferedImage, pixelCount: Int): (BufferedImage, Int) = {
    logInfo("File Overlap:" + inputMBR)
    var curPixelCount = pixelCount
    reader.initialize(rasterPath.getFileSystem(new Configuration()), rasterPath.toString, "0", new BeastOptions())
    val transformedInputTL = convertCoordinates(new Coordinate(inputMBR.x1, inputMBR.y1), revTransform)
    val transformedInputBR = convertCoordinates(new Coordinate(inputMBR.x2, inputMBR.y2), revTransform)
    val inputMBRRev: Rectangle = new Rectangle(transformedInputTL.x, transformedInputTL.y, transformedInputBR.x, transformedInputBR.y)

    val startTile: Point2D.Double = new Point2D.Double()
    reader.metadata.modelToGrid(inputMBRRev.x1, inputMBRRev.y1, startTile)
    val endTile: Point2D.Double = new Point2D.Double()
    reader.metadata.modelToGrid(inputMBRRev.x2, inputMBRRev.y2, endTile)

    val tileGrid2imgGrid: MathTransform = concatenateTransforms(new AffineTransform2D(reader.metadata.g2m), transform, new AffineTransform2D(tgtMetadata.g2m.createInverse()))
    val imgGrid2tileGrid: MathTransform = tileGrid2imgGrid.inverse()

    val tileList: Seq[Int] = getTileIDsInRectangle(startTile, endTile, reader.metadata)
    val size = tileList.size
    logInfo(s"Execution TileList size : $size ")
    for (tile <- tileList) {
      val sourceTile = reader.readTile(tile)
      val tileCorners = Array[Double](sourceTile.x1, sourceTile.y1,
        sourceTile.x2 + 1, sourceTile.y1,
        sourceTile.x2 + 1, sourceTile.y2 + 1,
        sourceTile.x1, sourceTile.y2 + 1,
      )
      tileGrid2imgGrid.transform(tileCorners, 0, tileCorners, 0, 4)

      val targetRasterX1: Int = (((tileCorners(0) min tileCorners(2) min tileCorners(4) min tileCorners(6)) + 0.5) max 0).toInt
      val targetRasterX2: Int = (((tileCorners(0) max tileCorners(2) max tileCorners(4) max tileCorners(6)) + 0.5) min 255).toInt
      val targetRasterY1: Int = (((tileCorners(1) min tileCorners(3) min tileCorners(5) min tileCorners(7)) + 0.5) max 0).toInt
      val targetRasterY2: Int = (((tileCorners(1) max tileCorners(3) max tileCorners(5) max tileCorners(7)) + 0.5) min 255).toInt
      for {
        y <- targetRasterY1 to targetRasterY2
        x <- targetRasterX1 to targetRasterX2
      } {
        if (image.getRGB(x, y) == 0x00FFFFFF) {
          val point: DirectPosition = new DirectPosition2D(x, y)
          imgGrid2tileGrid.transform(point, point)
          val x1 = point.getOrdinate(0).toInt
          val y1 = point.getOrdinate(1).toInt
          sourceTile.pixelLocations
          if (x1 >= sourceTile.x1 && x1 <= sourceTile.x2 && y1 >= sourceTile.y1 && y1 <= sourceTile.y2 && !sourceTile.isEmpty(x1, y1)) {
            if (!sourceTile.isEmpty(x1, y1)) {
              val pixelValue: Array[Int] = sourceTile.getPixelValue(x1, y1)
              val c = new Color(pixelValue(0), pixelValue(1), pixelValue(2))
              image.setRGB(x, y, c.getRGB)
              curPixelCount += 1
            }
          }
        }
        if (curPixelCount == 65536) {
          reader.close()
          imageCount += 1
          return (image, curPixelCount)
        }
      }
    }
    (image, curPixelCount)
  }
}
