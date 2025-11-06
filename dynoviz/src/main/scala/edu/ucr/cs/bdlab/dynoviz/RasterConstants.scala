package edu.ucr.cs.bdlab.dynoviz

import edu.ucr.cs.bdlab.davinci.CommonVisualizationHelper

/**
 * Constants related to raster visualization.
 */
object RasterConstants {
  /** Spatial Reference Identifier for Web Mercator projection */
   val WEBMERCATOR_SRID: Int= 3857
   val WEBMERCATOR_ENVELOPE = CommonVisualizationHelper.MercatorMapBoundariesEnvelope

  /** Dimensions of individual tiles */
   val TILE_WIDTH: Int = 256
   val TILE_HEIGHT: Int = 256

  /** Spatial extent for the Web Mercator projection */
   val ESPG3857_X1: Double = WEBMERCATOR_ENVELOPE.getMinX
   val ESPG3857_Y1: Double = WEBMERCATOR_ENVELOPE.getMinY
   val ESPG3857_X2: Double = WEBMERCATOR_ENVELOPE.getMaxX
   val ESPG3857_Y2: Double = WEBMERCATOR_ENVELOPE.getMaxY
}
