package edu.ucr.cs.bdlab.dynoviz.raptorhunt

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging

object IndexReader extends Logging{

  case class CsvRow(fileName: String, SRID: Int, x1: Double, y1: Double, x2: Double, y2: Double, wkt: String){
    override def toString: String = {
      s"$fileName, $SRID, $x1, $y1, $x2, $y2, $wkt"
    }
  }

  def checkIfIndexExists(directoryPath: String, fs: FileSystem): Boolean = {
    try {
      val basePath = new Path(directoryPath)
      if (fs.exists(basePath) && fs.isDirectory(basePath)) {
        val fileStatuses = fs.listStatus(basePath)
        fileStatuses.exists { fileStatus =>
          fileStatus.isFile &&
            fileStatus.getPath.getName.startsWith("_index") &&
            fileStatus.getPath.getName.toLowerCase.endsWith(".csv")
        }
      } else {
        false
      }
    } catch {
      case e: Exception =>
        e.printStackTrace()
        logError(s"Exception Occurred while checking if there is an index csv file for the directory")
        false
    }
  }

  def removeSpaces(str: String): String = {
    str.replaceAll("\\s", "")
  }

}
