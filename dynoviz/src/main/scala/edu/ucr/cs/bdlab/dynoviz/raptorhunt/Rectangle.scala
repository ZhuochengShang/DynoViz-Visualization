package edu.ucr.cs.bdlab.dynoviz.raptorhunt

import org.locationtech.jts.geom.Coordinate

/**
 * Class representing a rectangle with top-left corner (x1, y1) and bottom-right corner (x2, y2).
 *
 * @param x1 The x-coordinate of the top-left corner.
 * @param y1 The y-coordinate of the top-left corner.
 * @param x2 The x-coordinate of the bottom-right corner.
 * @param y2 The y-coordinate of the bottom-right corner.
 */
class Rectangle(val x1: Double, val y1: Double, val x2: Double, val y2: Double) {
  /**
   * Returns a string representation of the rectangle.
   *
   * @return A string representing the rectangle in the format "Rectangle(x1, y1, x2, y2)".
   */
  override def toString: String = f"MBR ($x1%.2f, $y1%.2f, $x2%.2f, $y2%.2f)"

  /**
   * Checks if the given point is within this rectangle.
   *
   * @param point The point to check.
   * @return true if the point is within the rectangle, false otherwise.
   */
  def containsPoint(point: Coordinate): Boolean = {
    x1 <= point.x && point.x <= x2 && y1 >= point.y && point.y >= y2
  }
}
