package edu.ucr.cs.bdlab.dynoviz

import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.davinci.{CommonVisualizationHelper, MultilevelPyramidPlotHelper, SingleLevelPlotHelper}
import org.apache.hadoop.io.Text
import org.apache.hadoop.util.LineReader
import org.apache.spark.internal.Logging

import java.io.UnsupportedEncodingException
import java.net.URLEncoder

object HTMLGenerator extends Logging {

  /**
   * Generates an HTML file based on BeastOptions configuration and template stored in resources/zoom_view.html
   * It makes use open Layers map and is use to visualize the tiles generated as an overlay
   *
   * @param opts BeastOptions containing configuration for HTML generation.
   * @param datasetPath Input Dataset path
   * @param workingDir Output path
   * @return String containing the HTML content.
   */
  def getIndexHTMLFile(opts: BeastOptions, datasetPath: String, workingDir: String): String = {
    val tileWidth: Int = opts.getInt(SingleLevelPlotHelper.ImageWidth, 256)
    val tileHeight: Int = opts.getInt(SingleLevelPlotHelper.ImageHeight, 256)
    val mercator: Boolean = opts.getBoolean(CommonVisualizationHelper.UseMercatorProjection, defaultValue = true)
    val strLevels: Array[String] = opts.getString("levels", "15").split("\\.\\.")
    var minLevel: Int = 0
    var maxLevel: Int = 0
    if (strLevels.length == 1) {
      minLevel = 0
      maxLevel = strLevels(0).toInt - 1
    }
    else {
      minLevel = strLevels(0).toInt
      maxLevel = strLevels(1).toInt
    }
    val templateFile = "/zoom_view_png.html"
    val templateFileReader = new LineReader(classOf[MultilevelPyramidPlotHelper].getResourceAsStream(templateFile))
    val htmlOut = new StringBuilder
    try {
      val line = new Text
      val imageExtension = ".png"
      while ( {
        templateFileReader.readLine(line) > 0
      }) {
        var lineStr = line.toString
        lineStr = lineStr.replace("#{MERCATOR}", mercator.toString)
        lineStr = lineStr.replace("#{TILE_WIDTH}", Integer.toString(tileWidth))
        lineStr = lineStr.replace("#{TILE_HEIGHT}", Integer.toString(tileHeight))
        lineStr = lineStr.replace("#{MAX_ZOOM}", Integer.toString(maxLevel))
        lineStr = lineStr.replace("#{MIN_ZOOM}", Integer.toString(minLevel))
        lineStr = lineStr.replace("#{TILE_URL}", "tile-{z}-{x}-{y}" + imageExtension)
        lineStr = lineStr.replace("#{DATASET_PATH}", (datasetPath))
        lineStr = lineStr.replace("#{WORKING_DIR}", (workingDir))
        htmlOut.append(lineStr)
        htmlOut.append("\n")
      }
    } finally {
      templateFileReader.close()
    }
    htmlOut.toString()
  }

  /**
   * URL encodes the given string using UTF-8 encoding.
   *
   * @param urlToEncode the string to URL encode
   * @return the URL-encoded string, or an empty string if an UnsupportedEncodingException occurs
   */
  def urlEncode(urlToEncode: String): String = {
    try {
      URLEncoder.encode(urlToEncode, "UTF-8")
    } catch {
      case _: UnsupportedEncodingException => ""
    }
  }


}
