package edu.ucr.cs.bdlab.dynoviz

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterMetadata}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.types.IntegerType
import org.jetbrains.bio.npy.NpyFile

import java.io._
import java.nio.file.Paths



object NPYUtils extends Logging {

  /**
   * Generates a NPY file based on tile information and saves it to the given directory on disk.
   *
   * @param tile        The tile containing pixel values.
   * @param metadata    RasterMetadata containing information about the raster.
   * @param zoomLevel   The zoom level of the tile.
   * @param outputDirPath Path to the output directory where the PNG will be saved.
   * @param opts        BeastOptions for configuration.
   */
  def tileToNpyBytes[T](tile: ITile[T], metadata: RasterMetadata, zoomLevel: Int, outputDirPath: String, opts: BeastOptions): Unit = {
    // Determine the size of the tile and allocate an array to hold its data
    val row = tile.tileID / metadata.numTilesY
    val column = tile.tileID % metadata.numTilesY
    val outputNPYFilename = s"tile-$zoomLevel-$column-$row.npy"
    print(outputNPYFilename)

    val shape = if (tile.numComponents == 1) {
      Array[Int](tile.tileWidth, tile.tileHeight)
    } else {
      Array[Int](tile.numComponents, tile.tileWidth, tile.tileHeight)
    }
    val npyFile: File = new File(outputDirPath,outputNPYFilename)
    val size = shape.product

    tile.componentType match {
      case IntegerType if tile.numComponents == 1 =>
        val data = new Array[Int](size)
        for ((x, y, value) <- tile.pixels) {
          val index = ((y * tile.tileWidth) + x) * tile.numComponents
          data(index) = value.asInstanceOf[Int]
        }
        NpyFile.write(Paths.get(npyFile.toString), data, shape, java.nio.ByteOrder.nativeOrder())
      case IntegerType =>
        val data = new Array[Int](size)
        val pixelLocationIterator = tile.pixels

        pixelLocationIterator.foreach { case (x, y, value) =>
           try {
             val adjustedY = y - tile.y1
             val adjustedX = x - tile.x1
              val pixelValue: Array[Int] = value.asInstanceOf[Array[Int]]
              val index = adjustedY * tile.tileWidth + adjustedX
              var band = 0
              for (v <- pixelValue) {
                val updatedIndex = index + (tile.tileWidth * tile.tileHeight * band)
                data(updatedIndex) = v
                band += 1
                if (updatedIndex == size - 1)
                  print("fullfill: " + updatedIndex)
              }
            } catch {
              case e: Exception =>
                logError(s"Exception occurred while processing pixel at ($x, $y): ${e.getMessage}")
            }
        }
        NpyWriter.writeNpyFile(data, shape, npyFile.toPath.toString)
    }
  }
}
