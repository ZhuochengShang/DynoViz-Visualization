package edu.ucr.cs.bdlab.dynoviz

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import org.apache.hadoop.fs.{FSDataOutputStream, Path}
import org.apache.spark.internal.Logging

import java.awt.Color
import java.awt.image.BufferedImage
import javax.imageio.ImageIO

object PNGUtils extends Logging {

  /**
   * Generates a PNG image based on tile information and saves it to the given directory on disk.
   *
   * @param tile        The tile containing pixel values.
   * @param metadata    RasterMetadata containing information about the raster.
   * @param zoomLevel   The zoom level of the tile.
   * @param outputDirPath Path to the output directory where the PNG will be saved.
   * @param opts        BeastOptions for configuration.
   */
  def generatePNG(tile: ITile[Array[Int]], metadata: RasterMetadata, zoomLevel: Int, outputDirPath: String, opts: BeastOptions): Unit = {
    val width = metadata.tileWidth
    val height = metadata.tileHeight
    val image = new BufferedImage(width, height, BufferedImage.TYPE_INT_ARGB)
    val pixelLocationIterator = tile.pixelLocations

    pixelLocationIterator.foreach { case (x, y) =>
      val adjustedX = x - tile.x1
      val adjustedY = y - tile.y1
      if (tile.isEmpty(x, y)) {
        image.setRGB(adjustedX, adjustedY, 0x00FFFFFF)
      }
      else {
        try {
          val pixelValue: Array[Int] = tile.getPixelValue(x, y)
          val c = new Color(pixelValue(0), pixelValue(1), pixelValue(2))
          image.setRGB(adjustedX, adjustedY, c.getRGB)
        } catch {
          case e: Exception =>
            logError(s"Exception occurred while processing pixel at ($x, $y): ${e.getMessage}")
            image.setRGB(adjustedX, adjustedY, 0x00FFFFFF)
        }
      }
    }

    val row = tile.tileID / metadata.numTilesY
    val column = tile.tileID % metadata.numTilesY
    val outputFilename = s"tile-$zoomLevel-$column-$row.png"

    try {
      val outFS = new Path(outputDirPath).getFileSystem(opts.loadIntoHadoopConf())
      val outputPath = new Path(outputDirPath, outputFilename)
      logDebug(s"Attempting to write $outputFilename to: ${outputPath.toString} in ${outFS.toString}")
      val outputStream: FSDataOutputStream = outFS.create(outputPath)
      ImageIO.write(image, "png", outputStream)
      outputStream.close()
    } catch {
      case e: Exception =>
        logError(s"Exception occurred while writing PNG image: ${e.getMessage}")
        throw e
    }
  }
}
