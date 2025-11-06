package edu.ucr.cs.bdlab.dynoviz.raptorhunt

import edu.ucr.cs.bdlab.beast.geolite.RasterMetadata
import edu.ucr.cs.bdlab.dynoviz.RasterConstants
import org.geotools.geometry.jts.JTS
import org.geotools.referencing.operation.transform.ConcatenatedTransform
import org.locationtech.jts.geom.Coordinate
import org.opengis.referencing.operation.MathTransform

import java.awt.geom.Point2D

object RaptorUtils {
  /**
   * Determines whether two rectangles overlap.
   *
   * @param rectangle1 The first rectangle.
   * @param rectangle2 The second rectangle.
   * @return `true` if the rectangles overlap, `false` otherwise.
   */
  def doOverlap(rectangle1: Rectangle, rectangle2: Rectangle): Boolean = {
    if (rectangle1.x1 == rectangle1.x2 || rectangle1.y1 == rectangle1.y2 ||
      rectangle2.x1 == rectangle2.x2 || rectangle2.y1 == rectangle2.y2) {
      return false
    }

    if (rectangle1.x1 > rectangle2.x2 || rectangle2.x1 > rectangle1.x2) {
      return false
    }

    if (rectangle1.y2 > rectangle2.y1 || rectangle2.y2 > rectangle1.y1) {
      return false
    }
    true
  }

  /**
   * Generates a sequence of tile IDs that overlap with the specified rectangle defined by two points.
   *
   * @param startTile The top-left point of the rectangle in tile space.
   * @param endTile   The bottom-right point of the rectangle in tile space.
   * @param metadata  The raster metadata containing information about the tiles.
   * @return A sequence of tile IDs that overlap with the specified rectangle.
   */
  def getTileIDsInRectangle(startTile: Point2D, endTile: Point2D, metadata: RasterMetadata): Seq[Int] = {
    /**
     * Clamps a value within the specified range.
     *
     * @param value The value to clamp.
     * @param min   The minimum allowed value.
     * @param max   The maximum allowed value.
     * @return The clamped value.
     */
    def clamp(value: Int, min: Int, max: Int): Int = {
      math.max(min, math.min(value, max))
    }

    val startITile = clamp(startTile.getX.toInt / metadata.tileWidth, 0, metadata.numTilesX - 1)
    val startJTile = clamp(startTile.getY.toInt / metadata.tileHeight, 0, metadata.numTilesY - 1)
    val endITile = clamp(endTile.getX.toInt / metadata.tileWidth, 0, metadata.numTilesX - 1)
    val endJTile = clamp(endTile.getY.toInt / metadata.tileHeight, 0, metadata.numTilesY - 1)

    val tileList = for {
      j <- startJTile to endJTile
      i <- startITile to endITile
    } yield j * metadata.numTilesX + i

    tileList
  }

  /**
   * Converts a [[Coordinate]] using a given [[MathTransform]].
   *
   * This method transforms the input <pre>coordinate</pre> using the specified
   * <pre>transform</pre> and returns a new <pre>Coordinate</pre> with the transformed
   * coordinates.
   *
   * @param coordinate The input coordinate to be transformed.
   * @param transform  The math transform used for coordinate conversion.
   * @return A new <pre>Coordinate</pre> representing the transformed coordinates.
   * @see org.locationtech.jts.geom.Coordinate
   * @see org.locationtech.jts.transform.MathTransform
   * @see org.locationtech.jts.transform.CoordinateTransform
   * @since 1.0
   */
  def convertCoordinates(coordinate: Coordinate, transform: MathTransform): Coordinate = {
    val temp = new Coordinate
    JTS.transform(coordinate, temp, transform)
    temp
  }

  /**
   * Determines whether a point is within a rectangle.
   *
   * @param point     The coordinate representing the point.
   * @param rectangle The rectangle to check against.
   * @return `true` if the point is within the rectangle, `false` otherwise.
   */
   def isPointInRectangle(point: Coordinate, rectangle: Rectangle): Boolean = {
    val xInRange = point.x >= rectangle.x1 && point.x <= rectangle.x2
    val yInRange = point.y <= rectangle.y1 && point.y >= rectangle.y2
    xInRange && yInRange
  }


  /**
   * Validates the bounds of x and y values against a given zoom level.
   *
   * @param zoomLevel The zoom level for raster generation.
   * @param x         The column value to check against the zoom level bounds.
   * @param y         The row value to check against the zoom level bounds.
   * @throws IllegalArgumentException if x or y exceeds the allowed bounds for the given zoom level.
   */
  def validateXYBoundsForZoomLevel(zoomLevel: Int, x: Int, y: Int): Unit = {
    val maxAllowedValue = math.pow(2, zoomLevel).toInt

    if (x >= maxAllowedValue || y >= maxAllowedValue) {
      if (x >= maxAllowedValue && y >= maxAllowedValue) {
        throw new IllegalArgumentException(s"x and y are out of bounds for the given zoom level")
      } else if (x >= maxAllowedValue) {
        throw new IllegalArgumentException(s"x is out of bounds for the given zoom level")
      } else {
        throw new IllegalArgumentException(s"y is out of bounds for the given zoom level")
      }
    }
  }

  /**
   * Calculates the spatial extent of a specific tile at a given zoom level, x, and y
   * in the Mercator-projected EPSG:3857 coordinate system.
   *
   * @param zoomLevel The zoom level for raster generation.
   * @param x         The column value of the tile.
   * @param y         The row value of the tile.
   * @return A tuple (Rectangle) representing the spatial extent: (startX, endX, startY, endY).
   */
  def calculateSpatialExtent(zoomLevel: Int, x: Int, y: Int): Rectangle = {
    val rowColCount = math.pow(2, zoomLevel)

    val tileWidth = (RasterConstants.ESPG3857_X2 - RasterConstants.ESPG3857_X1) / rowColCount
    val tileHeight = (RasterConstants.ESPG3857_Y1 - RasterConstants.ESPG3857_Y2) / rowColCount

    val startX = RasterConstants.ESPG3857_X1 + x * tileWidth
    val endX = RasterConstants.ESPG3857_X1 + (x + 1) * tileWidth
    val startY = RasterConstants.ESPG3857_Y1 - y * tileHeight
    val endY = RasterConstants.ESPG3857_Y1 - (y + 1) * tileHeight

    new Rectangle(startX, startY, endX, endY)
  }

  /**
   * Calculates the tile ID corresponding to a given point within the raster metadata.
   *
   * @param metadata The raster metadata containing information about the tiles.
   * @param pointX   The X-coordinate of the point.
   * @param pointY   The Y-coordinate of the point.
   * @return The tile ID corresponding to the given point.
   */
  def calculateTileIDForPoint(metadata: RasterMetadata, pointX: Double, pointY: Double): Int = {
    val tileWidth = metadata.tileWidth
    val tileHeight = metadata.tileHeight
    val temp = new Point2D.Double
    metadata.modelToGrid(pointX, pointY, temp)

    val columnIndex = Math.floor((temp.x - metadata.x1) / tileWidth).toInt
    val rowIndex = Math.floor((temp.y - metadata.y1) / tileHeight).toInt

    val tileId = rowIndex * metadata.numTilesX + columnIndex
    tileId
  }

  /**
   * Concatenates a set of MathTransforms into a single MathTransform.
   *
   * @param transforms The list of MathTransforms to concatenate into one.
   * @return The concatenated MathTransform.
   */
  def concatenateTransforms(transforms: MathTransform*): MathTransform =
    transforms.reduceLeft(ConcatenatedTransform.create)

}
