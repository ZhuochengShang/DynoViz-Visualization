package edu.ucr.cs.bdlab.dynoviz.raptorhunt

import org.geotools.referencing.CRS
import org.opengis.referencing.crs.CoordinateReferenceSystem
import org.opengis.referencing.operation.MathTransform

import scala.collection.mutable


object MathTransformCache{
  private val mathTransformCache: mutable.HashMap[String, MathTransform] = mutable.HashMap()

  def getTransform(sourceCRS: CoordinateReferenceSystem, targetCRS: CoordinateReferenceSystem): MathTransform = {
    val key = s"${sourceCRS.getName}|${targetCRS.getName}"
    mathTransformCache.getOrElseUpdate(key, CRS.findMathTransform(sourceCRS, targetCRS))
  }
}