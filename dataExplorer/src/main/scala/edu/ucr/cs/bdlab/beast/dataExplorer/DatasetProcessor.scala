package edu.ucr.cs.bdlab.beast.dataExplorer

import com.fasterxml.jackson.core.{JsonFactory, JsonGenerator}
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.SpatialRDD
import edu.ucr.cs.bdlab.beast.util.{FileUtil, WebUtil}
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import edu.ucr.cs.bdlab.beast._
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.indexing.{IndexHelper, RSGrovePartitioner}
import edu.ucr.cs.bdlab.beast.io.{SpatialCSVSource, SpatialParquetSource}
import edu.ucr.cs.bdlab.beast.synopses.Summary
import edu.ucr.cs.bdlab.davinci.MVTDataVisualizer
import org.apache.hadoop.io.IOUtils
import org.apache.spark.beast.sql.GeometryDataType
import org.apache.spark.sql.types.StructField

import java.io.{ByteArrayOutputStream, File, PrintStream}
import java.sql.{Connection, PreparedStatement, ResultSet}
import java.util.zip.{GZIPInputStream, ZipFile}
import scala.collection.JavaConverters.mapAsScalaMapConverter

/**
 * A processor for handling datasets, including loading, copying to local storage, decompressing,
 * summarizing, building indexes, and visualizing. It updates the database as it progresses through the steps.
 * If an error happens at any step, it stops and writes the error to the database for that dataset.
 *
 * @param datasetName The name of the dataset being processed.
 * @param dbConnection Connection to the database.
 * @param datasetsDir Directory where datasets are stored.
 * @param datasetFS File system abstraction for dataset operations.
 * @param sparkSession Spark session for data processing tasks.
 */
class DatasetProcessor(datasetName: String, dbConnection: java.sql.Connection, datasetsDir: String,
                       datasetFS: FileSystem, sparkSession: SparkSession) extends Logging {

  import DatasetProcessor._

  val attributes: Map[String, Any] = using(dbConnection.prepareStatement("SELECT * FROM datasets WHERE name = ?")) { preparedStatement =>
    preparedStatement.setString(1, datasetName)
    val resultSet = preparedStatement.executeQuery()
    if (!resultSet.next())
      throw new RuntimeException(s"Cannot find dataset $datasetName")
    Map[String, Any](
      "id" -> resultSet.getInt("id"),
      "dir_name" -> resultSet.getString("dir_name"),
      "copy_path" -> resultSet.getString("copy_path"),
      "index_path" -> resultSet.getString("index_path"),
      "viz_options" -> resultSet.getString("viz_options"),
      "progress" -> resultSet.getInt("progress"),
      "status" -> resultSet.getString("status")
    )
  }

  def id: Int = attributes("id").asInstanceOf[Int]

  lazy val datasetPath: Path = new Path(datasetsDir, attributes("dir_name").toString)

  lazy val dataIndexPath: Path = new Path(datasetPath, {
    var indexPath: String = attributes("index_path").asInstanceOf[String]
    if (indexPath == null) {
      indexPath = "data_index"
      // Dataset directory not set. Set to default and return it
      using(dbConnection.prepareStatement("UPDATE datasets SET index_path=? WHERE name=?")) {updateStatement =>
        updateStatement.setString(1, indexPath)
        updateStatement.setString(2, datasetName)
        updateStatement.executeUpdate()
      }
    }
    indexPath
  })

  def visualizationPath: Path = new Path(datasetPath, {
    var viz_opts_string: String = attributes("viz_options").asInstanceOf[String]
    if (viz_opts_string != null) {
      val objectMapper = new ObjectMapper()
      val parsed_opts = objectMapper.readTree(viz_opts_string)
      val viz_opts = WebUtil.jsonNodeToScala(parsed_opts).asInstanceOf[Map[String, Any]]
      if (viz_opts.contains("viz_path")) viz_opts("viz_path").toString else "plot.zip"
    } else {
      "plot.zip"
    }
  })

  def deleteAllFiles(): Boolean = datasetFS.delete(datasetPath, true)

  def deleteFromDB(): Boolean = {
    using(dbConnection.prepareStatement("DELETE FROM datasets WHERE name = ?")) { deleteStatement =>
      deleteStatement.setString(1, datasetName)
      deleteStatement.executeUpdate() > 0
    }
  }

  /**
   * Loads the raw (non-indexed) data from the local copy
   * @return
   */
  private def loadRawData: SpatialRDD = {
    using(dbConnection.prepareStatement("SELECT * FROM datasets WHERE name = ?")) { preparedStatement =>
      preparedStatement.setString(1, datasetName)
      val resultSet: ResultSet = preparedStatement.executeQuery()
      if (!resultSet.next())
        throw new RuntimeException(s"Dataset '$datasetName' not found in DB")
      val format: String = resultSet.getString("source_format")
      if (!resultSet.wasNull() && format == "sql") {
        // SQL query
        val sqlQuery = resultSet.getString("source_uri")
        val tableNames: Set[String] = SQLQueryHelper.extractTables(sqlQuery)
        for (tableName <- tableNames) {
          using(dbConnection.prepareStatement("SELECT name, status FROM datasets WHERE LOWER(name) = LOWER(?)")) { statement =>
            statement.setString(1, tableName)
            using (statement.executeQuery()) { dataset =>
              if (!dataset.next())
                throw new RuntimeException(s"Dataset '$datasetName depends on a non-existent dataset $tableName")
              val status = dataset.getString("status")
              if ("ready" != status)
                throw new RuntimeException(s"Dataset '$datasetName depends on a non-ready dataset $tableName")
              val subDatasetName = dataset.getString("name")
              val datasetP = new DatasetProcessor(subDatasetName, dbConnection, datasetsDir, datasetFS, sparkSession)
              datasetP.loadIndexedData.toDataFrame(sparkSession).createOrReplaceTempView(tableName)
            }
          }
        }
        // All datasets are loaded, now run this query and return a spatial RDD
        sparkSession.sql(sqlQuery).toSpatialRDD
      } else {
        // Load from file
        val copy_path = new Path(datasetPath, resultSet.getString("copy_path"))
        val beastOptionsStr = resultSet.getString("beast_options")
        var beastOptions: Map[String, String] = if (resultSet.wasNull()) Map() else {
          val objectMapper = new ObjectMapper()
          WebUtil.jsonNodeToScala(objectMapper.readTree(beastOptionsStr)).asInstanceOf[Map[String, Any]]
            .map(x => (x._1, x._2.toString))
        }
        beastOptions = beastOptions + ("recursiveFileLookup" -> "true")
        val reader = sparkSession.read.options(beastOptions)
        val format: String = resultSet.getString("source_format")
        val spatialDF: DataFrame = format match {
          case "shapefile" => reader.format("shapefile").load(copy_path.toString)
          case "gpx" => reader.format("gpx").load(copy_path.toString)
          case "geojson" => reader.format("geojson").load(copy_path.toString)
          case "csv" =>
            beastOptions = beastOptions + ("inferSchema" -> "true")
            SpatialCSVSource.read(sparkSession, copy_path.toString, beastOptions)
          case other =>
            throw new RuntimeException(s"Unrecognized input format '$other'")
        }
        spatialDF.toSpatialRDD
      }
    }
  }

  private[dataExplorer] def copyDataToLocal(): Unit = {
    using(dbConnection.prepareStatement("SELECT * FROM datasets WHERE name = ?")) { preparedStatement =>
      preparedStatement.setString(1, datasetName)
      val resultSet: ResultSet = preparedStatement.executeQuery()

      if (!resultSet.next())
        throw new RuntimeException(s"Dataset '$datasetName' not found in DB")
      // Dataset has been created. Copy the input file to a temporary directory for processing
      val source_uris: Array[String] = resultSet.getString("source_uri").split("\n")
      val copy_path = new Path(datasetPath, "download")
      for (source_uri <- source_uris)
        WebUtil.downloadFile(source_uri, copy_path, datasetFS)
      val status = "copied"
      using(dbConnection.prepareStatement("UPDATE datasets SET copy_path=?, status=?, progress=20 WHERE name=?")) { updateDataset =>
        updateDataset.setString(1, copy_path.getName)
        updateDataset.setString(2, status)
        updateDataset.setString(3, datasetName)
        updateDataset.executeUpdate()
      }
    }
  }

  def geometryType: String = {
    using(dbConnection.prepareStatement("SELECT geometry_type FROM datasets WHERE name = ?")) { preparedStatement =>
      preparedStatement.setString(1, datasetName)
      val resultSet: ResultSet = preparedStatement.executeQuery()

      if (!resultSet.next())
        throw new RuntimeException(s"Dataset '$datasetName' not found in DB")

      resultSet.getString("geometry_type")
    }
  }

  /**
   * Decompresses dataset files that are stored locally. Specifically, it decompress any ZIP files found
   * in the dataset's path. It deletes those ZIP files and finally updates the dataset's status in the
   * database after decompression.
   */
  private[dataExplorer] def decompressDatasetFiles(): Unit = {
    using(dbConnection.prepareStatement("SELECT * FROM datasets WHERE name = ?")) { preparedStatement =>
      preparedStatement.setString(1, datasetName)
      val resultSet: ResultSet = preparedStatement.executeQuery()

      if (!resultSet.next())
        throw new RuntimeException(s"Dataset '$datasetName' not found in DB")

      val copy_path: Path = new Path(datasetPath, resultSet.getString("copy_path"))
      // Dataset is available for processing. Next step is to extract any ZIP files.
      decompressFiles(datasetFS, copy_path)

      val status = "decompressed"
      using(dbConnection.prepareStatement("UPDATE datasets SET status=?, progress=40 WHERE name=?")) { updateDataset =>
        updateDataset.setString(1, status)
        updateDataset.setString(2, datasetName)
        updateDataset.executeUpdate()
      }
    }
  }

  /**
   * Summarize the dataset and set the corresponding attributes in the dataset.
   */
  private[dataExplorer] def summarizeData(): Unit = {
    using(dbConnection.prepareStatement("SELECT * FROM datasets WHERE name = ?")) { preparedStatement =>
      preparedStatement.setString(1, datasetName)
      val resultSet: ResultSet = preparedStatement.executeQuery()

      if (!resultSet.next())
        throw new RuntimeException(s"Dataset '$datasetName' not found in DB")

      val rawData: SpatialRDD = loadRawData
      val summary = rawData.summary
      val status = "summarized"
      val updateSQL = "UPDATE datasets SET status=?, progress=50, geometry_type=?, MBR=?, size=?, num_features=?, num_points=?, schema=? WHERE name=?"
      using(dbConnection.prepareStatement(updateSQL)) { updateSummary =>
        updateSummary.setString(1, status)
        val mbrArray = dbConnection.createArrayOf("REAL", Array(summary.getMinCoord(0), summary.getMinCoord(1),
          summary.getMaxCoord(0), summary.getMaxCoord(1)).map(_.asInstanceOf[AnyRef]))
        updateSummary.setString(2, summary.geometryType.toString)
        updateSummary.setArray(3, mbrArray)
        updateSummary.setLong(4, summary.size)
        updateSummary.setLong(5, summary.numFeatures)
        updateSummary.setLong(6, summary.numPoints)
        val schema = rawData.first().schema
        val baos = new ByteArrayOutputStream()
        val generator: JsonGenerator = new JsonFactory().createGenerator(baos)
        Summary.writeSchemaAsJSON(schema, generator)
        generator.close()
        updateSummary.setString(7, new String(baos.toByteArray))
        updateSummary.setString(8, datasetName)
        updateSummary.executeUpdate()
      }
    }
  }

  private[dataExplorer] def buildIndexes(): Unit = {
    using(dbConnection.prepareStatement("SELECT * FROM datasets WHERE name = ?")) { preparedStatement =>
      preparedStatement.setString(1, datasetName)
      val resultSet: ResultSet = preparedStatement.executeQuery()

      if (!resultSet.next())
        throw new RuntimeException(s"Dataset '$datasetName' not found in DB")

      val rawData: SpatialRDD = loadRawData.zipWithUniqueId().map(fid => {
        edu.ucr.cs.bdlab.beast.geolite.Feature.append(fid._1, fid._2, "_id")
      })
      val partitionedData = IndexHelper.partitionFeatures2(rawData, classOf[RSGrovePartitioner], f => f.getStorageSize,
        Seq(IndexHelper.BalancedPartitioning -> false))
      partitionedData.writeSpatialFile(dataIndexPath.toString, "rtree")
      val status = "indexed"
      val updateSQL = "UPDATE datasets SET status=?, progress=80, index_path=? WHERE name=?"
      using(dbConnection.prepareStatement(updateSQL)) { updateSummary =>
        updateSummary.setString(1, status)
        updateSummary.setString(2, dataIndexPath.getName)
        updateSummary.setString(3, datasetName)
        updateSummary.executeUpdate()
      }
    }
  }

  private def loadIndexedData: SpatialRDD = {
    using(dbConnection.prepareStatement("SELECT * FROM datasets WHERE name = ?")) { preparedStatement =>
      preparedStatement.setString(1, datasetName)
      val resultSet: ResultSet = preparedStatement.executeQuery()

      if (!resultSet.next())
        throw new RuntimeException(s"Dataset '$datasetName' not found in DB")

      sparkSession.sparkContext.spatialFile(dataIndexPath.toString, "rtree")
    }
  }

  /**
   * Returns the following visualization options for this dataset in a dictionary (Map object). Dots indicate nesting.
   *  - *visualization_path*: The path to where the visualization index is stored. Typically, a ZIP file.
   *  - *visualization_type*: The type of visualization, e.g., VectorTile, RasterTile, or GeoJSON.
   *  - *style*: A style object or JavaScript code that returns a style object to use in OpenLayers. Only used with
   *    VectorTile and GeoJSON.
   *  - *beast_options*: Additional options passed by the user for this dataset. It might or might not be relevant
   *    to visualization.
   * @return
   */
  def visualizationOptions: Map[String, Any] = {
    using(dbConnection.prepareStatement("SELECT * FROM datasets WHERE name = ?")) { preparedStatement =>
      preparedStatement.setString(1, datasetName)
      val resultSet: ResultSet = preparedStatement.executeQuery()

      if (!resultSet.next())
        throw new RuntimeException(s"Dataset '$datasetName' not found in DB")

      // An object to hold final results
      val objectMapper = new com.fasterxml.jackson.databind.ObjectMapper()
      var finalResult = Map[String, Any]()

      var opts_str: String = resultSet.getString("beast_options")
      if (!resultSet.wasNull()) {
        val parsed_opts = objectMapper.readTree(opts_str)
        val parsedMap = WebUtil.jsonNodeToScala(parsed_opts)
        finalResult += ("beast_options" -> parsedMap)
        // TODO merge these options with the _visualization.properties file in the plot.zip file, if needed
      }
      opts_str = resultSet.getString("viz_options")
      if (!resultSet.wasNull()) {
        val parsed_opts = objectMapper.readTree(opts_str)
        val parsedMap = WebUtil.jsonNodeToScala(parsed_opts).asInstanceOf[Map[String, Any]]
        finalResult = finalResult ++ parsedMap
      }
      finalResult += ("visualization_path" -> visualizationPath.toString)
      finalResult
    }
  }

  def updateStyle(newStyle: Map[String, Any]): Boolean = {
    using(dbConnection.prepareStatement("SELECT viz_options FROM datasets WHERE name = ?")) { preparedStatement =>
      preparedStatement.setString(1, datasetName)
      val resultSet: ResultSet = preparedStatement.executeQuery()

      if (!resultSet.next())
        throw new RuntimeException(s"Dataset '$datasetName' not found in DB")

      val objectMapper = new com.fasterxml.jackson.databind.ObjectMapper()
      var viz_options: Map[String, Any] = Map()
      val opts_str = resultSet.getString("viz_options")
      if (!resultSet.wasNull()) {
        val parsed_opts = objectMapper.readTree(opts_str)
        viz_options = WebUtil.jsonNodeToScala(parsed_opts).asInstanceOf[Map[String, Any]]
      }
      viz_options = viz_options ++ newStyle

      using(dbConnection.prepareStatement("UPDATE datasets SET viz_options=? WHERE name=?")) { updateStmt =>
        updateStmt.setString(1, WebUtil.scalaMapToJsonString(viz_options))
        updateStmt.setString(2, datasetName)
        updateStmt.executeUpdate() > 0
      }
    }
  }

  /**
   * Build the visualization index for this dataset
   */
  private[dataExplorer] def visualize(): Unit = {
    using(dbConnection.prepareStatement("SELECT * FROM datasets WHERE name = ?")) { preparedStatement =>
      preparedStatement.setString(1, datasetName)
      val resultSet: ResultSet = preparedStatement.executeQuery()

      if (!resultSet.next())
        throw new RuntimeException(s"Dataset '$datasetName' not found in DB")

      if (resultSet.getLong("size") > 1024 * 1024) {
        // Build a multi-level visualization
        val indexedData: SpatialRDD = loadIndexedData
        val beast_options = beastOptions(resultSet)
        val tiles = MVTDataVisualizer.plotAllTiles(indexedData, 0 until 20, 256, 5, beast_options)
        beast_options.set("data", FileUtil.relativize(dataIndexPath, visualizationPath))
        beast_options.set("levels", 20)
        beast_options.set("resolution", 256)
        beast_options.set("buffer", 5)
        MVTDataVisualizer.saveTilesCompact(tiles, visualizationPath.toString, beast_options)
        val status = "visualized"
        val updateSQL = "UPDATE datasets SET status=?, progress=90, viz_options=? WHERE name=?"
        using(dbConnection.prepareStatement(updateSQL)) { updateSummary =>
          updateSummary.setString(1, status)
          val viz_options = new ByteArrayOutputStream()
          val generator = new JsonFactory().createGenerator(viz_options)
          generator.writeStartObject()
          generator.writeStringField("viz_path", visualizationPath.getName)
          generator.writeStringField("viz_type", "VectorTile")
          generator.writeStringField("viz_url", s"/datasets/$datasetName/tiles/tile-{z}-{x}-{y}.mvt")
          generator.writeEndObject()
          generator.close()
          updateSummary.setString(2, new String(viz_options.toByteArray))
          updateSummary.setString(3, datasetName)
          updateSummary.executeUpdate()
        }
      } else {
        // Data is very small. No need to build a visualization index
        val status = "visualized"
        val updateSQL = "UPDATE datasets SET status=?, progress=90, viz_options=? WHERE name=?"
        using(dbConnection.prepareStatement(updateSQL)) { updateSummary =>
          updateSummary.setString(1, status)
          val viz_options = new ByteArrayOutputStream()
          val generator = new JsonFactory().createGenerator(viz_options)
          generator.writeStartObject()
          generator.writeStringField("viz_type", "GeoJSON")
          generator.writeStringField("viz_url", s"/datasets/$datasetName/download.geojson")
          generator.writeEndObject()
          generator.close()
          updateSummary.setString(2, new String(viz_options.toByteArray))
          updateSummary.setString(3, datasetName)
          updateSummary.executeUpdate()
        }
      }
    }
  }

  def exportDataset(format: String, path: String, conf: Map[String, String]): scala.concurrent.Future[Any] = {
    import scala.concurrent.{ExecutionContext, Future}
    implicit val ec: ExecutionContext = ExecutionContext.global
    // Step 1: Read the dataset
    val datasetDF = loadIndexedData.toDataFrame(sparkSession)

    val outPath = new Path(path)
    val fileSystem = outPath.getFileSystem(sparkSession.sparkContext.hadoopConfiguration)

    if (fileSystem.exists(outPath))
      throw new RuntimeException(s"Output path '$path' already exists")

    // Step 3: Determine the writer based on format
    val writer = format.toLowerCase match {
      case "geojson" =>
        datasetDF.write.format("geojson")
      case "geoparquet" =>
        SpatialParquetSource.encodeGeoParquet(datasetDF).write.format("parquet")
      case "spatialparquet" =>
        SpatialParquetSource.encodeSpatialParquet(datasetDF).write.format("parquet")
      case "shapefile" =>
        datasetDF.write.format("shapefile")
      case "json+wkt" =>
        val geomColumn: Option[StructField] = datasetDF.schema.find(_.dataType == GeometryDataType)
        if (geomColumn.isEmpty)
          throw new RuntimeException(s"Not geometry column in '${datasetDF.schema}'")
        datasetDF.withColumn(geomColumn.get.name, new org.apache.spark.sql.Column(s"ST_AsText(${geomColumn.get.name})"))
          .write.format("json")
      case _ => throw new UnsupportedOperationException(s"Export format $format is not supported.")
    }
    Future {writer.save(path)}
  }


  def run(): Unit = {
    var status: String = null
    try {
      // Retrieve dataset status
      val preparedStatement: PreparedStatement = dbConnection.prepareStatement("SELECT * FROM datasets WHERE name = ?")
      preparedStatement.setString(1, datasetName)
      var resultSet: ResultSet = preparedStatement.executeQuery()

      if (!resultSet.next()) {
        logWarning(s"Could not find dataset with name '$datasetName' to process")
        return
      }
      // Dataset found. Process it based on its status
      status = resultSet.getString("status")
      val format = resultSet.getString("source_format")
      val isSQL = !resultSet.wasNull() && format == "sql"
      resultSet.close()
      while (status != "ready") {
        resultSet = preparedStatement.executeQuery()
        resultSet.next()
        status = resultSet.getString("status")
        resultSet.close()

        status match {
          case "created" if !isSQL =>
            // Download a copy of dataset files locally for next steps
            copyDataToLocal()
          case "created" if isSQL =>
            // Skip to decompressed state
            status = "decompressed"
            val updateSQL = "UPDATE datasets SET status=?, progress=40 WHERE name=?"
            using(dbConnection.prepareStatement(updateSQL)) { updateSummary =>
              updateSummary.setString(1, status)
              updateSummary.setString(2, datasetName)
              updateSummary.executeUpdate()
            }
          case "copied" =>
            // Dataset is available for processing. Next step is to extract any ZIP files.
            decompressDatasetFiles()
          case "decompressed" =>
            // Dataset files are compressed. Next step is to summarize it
            summarizeData()
          case "summarized" =>
            // Dataset summarized. Now, build an index on the data
            buildIndexes()
          case "indexed" =>
            // Last step is to visualize
            visualize()
          case "visualized" =>
            // Make it ready
            status = "ready"
            val updateSQL = "UPDATE datasets SET status=?, progress=100 WHERE name=?"
            using(dbConnection.prepareStatement(updateSQL)) { updateReady =>
              updateReady.setString(1, status)
              updateReady.setString(2, datasetName)
              updateReady.executeUpdate()
            }
        }
      }
    } catch {
      case e: Throwable =>
        logError(s"Error processing dataset '$datasetName' in status '$status'", e)
        val errorMessage = new ByteArrayOutputStream()
        val out = new PrintStream(errorMessage)
        out.println(e.getMessage)
        e.printStackTrace(out)
        out.close()
        val msg = new String(errorMessage.toByteArray)
        val updateSQL = "UPDATE datasets SET error_message=? WHERE name=?"
        using(dbConnection.prepareStatement(updateSQL)) { updateDataset =>
          updateDataset.setString(1, msg)
          updateDataset.setString(2, datasetName)
          updateDataset.executeUpdate()
        }
    }
  }

}

object DatasetProcessor extends Logging {

  /**
   * Checks if the given dataset exists in the database
   * @param datasetName the name of the dataset
   * @param dbConnection the database connection
   * @return `true` if a dataset with the given name exists in the database
   */
  def datasetExists(datasetName: String, dbConnection: java.sql.Connection): Boolean = {
    using(dbConnection.prepareStatement("SELECT COUNT(1) FROM datasets WHERE name = ?")) { preparedStatement =>
      preparedStatement.setString(1, datasetName)
      val resultSet = preparedStatement.executeQuery()
      resultSet.next()
    }
  }

  /**
   * Decompress any ZIP files in the given directory and delete the ZIP file after it is extracted
   */
  private def decompressFiles(fs: FileSystem, downloadPath: Path): Unit = {
    var compressedFiles = false
    do {
      compressedFiles = false
      for (fileStatus <- fs.listStatus(downloadPath)) {
        if (fileStatus.isFile && FileUtil.extensionMatches(fileStatus.getPath.getName, ".zip")) {
          compressedFiles = true
          // Decompress a ZIP file
          var filename = fileStatus.getPath.getName
          val iLastDot = filename.lastIndexOf('.')
          if (iLastDot != -1) filename = filename.substring(0, iLastDot)
          else filename = filename + "_extracted"
          val extractDir = new Path(fileStatus.getPath.getParent, filename)
          fs.mkdirs(extractDir)
          // Open the Zip file
          // There is a problem with reading some ZIP files in a streaming fashion
          // https://stackoverflow.com/questions/47208272
          // To work around it, we copy the file to the local file system first
          var localZipFileName: File = null
          if (!fs.isInstanceOf[LocalFileSystem]) {
            // Copy the file to the local file system
            localZipFileName = File.createTempFile("tempdownload", "zip")
            fs.copyToLocalFile(fileStatus.getPath, new Path(localZipFileName.getPath))
          }
          else {
            // Already stored locally
            localZipFileName = new File(fileStatus.getPath.toUri.getPath)
          }
          val zipFile = new ZipFile(localZipFileName)
          try {
            val entries = zipFile.entries
            while (entries.hasMoreElements) {
              val zipEntry = entries.nextElement
              val expandedFileName = zipEntry.getName
              if (expandedFileName.endsWith("/")) {
              }
              else {
                val expandedFile = new Path(extractDir, expandedFileName)
                val out = fs.create(expandedFile)
                val zinStream = zipFile.getInputStream(zipEntry)
                IOUtils.copyBytes(zinStream, out, 1024 * 1024, true)
              }
            }
          } finally zipFile.close()
          // Delete the ZIP file after it is already expanded
          fs.delete(fileStatus.getPath, false)
        } else if (fileStatus.isFile && FileUtil.extensionMatches(fileStatus.getPath.getName, ".gz")) {
          // While most readers can process .gz files directly, they cannot be split so it's better to decompress them
          compressedFiles = true
          val in = new GZIPInputStream(fs.open(fileStatus.getPath))
          val expandedFileName = new Path(downloadPath, FileUtil.replaceExtension(fileStatus.getPath.getName, ""))
          val expandedOut = fs.create(expandedFileName)
          IOUtils.copyBytes(in, expandedOut, 1024 * 1024)
          expandedOut.close()
          in.close()
          // Delete the compressed file
          fs.delete(fileStatus.getPath, false)
        }
      }
    } while (compressedFiles)
  }

  def using[A <: AutoCloseable, B](resource: A)(block: A => B): B = {
    try {
      block(resource)
    } finally {
      if (resource != null) resource.close()
    }
  }

  /**
   * Returns the [[BeastOptions]] stored in the given [[ResultSet]] if set. Otherwise, a default one is returned.
   *
   * @param resultSet the result set that points to this dataset
   * @return
   */
  private def beastOptions(resultSet: ResultSet): BeastOptions = {
    val objectMapper = new ObjectMapper()
    objectMapper.registerModule(DefaultScalaModule)

    val opts = resultSet.getString("beast_options")
    if (resultSet.wasNull()) new BeastOptions() else {
      val mapType = objectMapper.getTypeFactory.constructMapType(classOf[java.util.Map[_, _]], classOf[String], classOf[String])
      objectMapper.readValue[java.util.Map[String, String]](opts, mapType).asScala
    }
  }


  def createDB(dbConnection: Connection): Unit = {
    // Check if we have the datasets table
    val metadata = dbConnection.getMetaData
    val resultSet = metadata.getTables(null, null, "DATASETS", null)
    try {
      if (!resultSet.next) {
        // Table does not exist, create
        logInfo("First time accessing the DatasetServer, creating the database")
        val statement = dbConnection.createStatement()
        statement.execute(
          """
          CREATE TABLE IF NOT EXISTS datasets(
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(255) UNIQUE NOT NULL,
            dir_name VARCHAR(255) UNIQUE NOT NULL,
            mbr REAL ARRAY[4],
            geometry_type VARCHAR(64),
            schema VARCHAR, -- Should be JSON
            size BIGINT,
            num_features BIGINT,
            num_points BIGINT,
            progress INT,
            status ENUM('created', 'copied', 'decompressed', 'summarized', 'indexed', 'visualized', 'ready', 'deleted'),
            error_message VARCHAR(65535),
            source_uri VARCHAR,
            source_format VARCHAR(255),
            beast_options VARCHAR, -- Should be JSON
            copy_path VARCHAR(255),
            index_path VARCHAR(255),
            viz_options VARCHAR -- Should be JSON
          )""")
        statement.execute(
          """
            CREATE TABLE IF NOT EXISTS workspaces(
              id INT AUTO_INCREMENT PRIMARY KEY,
              name VARCHAR(255) UNIQUE NOT NULL,
              description VARCHAR(65535)
            )""")
        statement.execute(
          """
            CREATE TABLE IF NOT EXISTS workspace_datasets(
              workspace_id INT,
              dataset_id INT,
              dataset_order INT,
              viz_style VARCHAR,
              PRIMARY KEY(workspace_id, dataset_id),
              FOREIGN KEY(workspace_id) REFERENCES workspaces(id),
              FOREIGN KEY(dataset_id) REFERENCES datasets(id)
            )""")
        statement.close()
        // Insert a default dataset for OSM layer
        val insertSQL = "INSERT INTO datasets(name, dir_name, mbr, status, progress, viz_options) VALUES (?, ?, ?, ?, 100, ?)"
        val insertStatement = dbConnection.prepareStatement(insertSQL)
        insertStatement.setString(1, "OSM")
        insertStatement.setString(2, "N/A")
        insertStatement.setArray(3, dbConnection.createArrayOf("REAL",
          Array(-180.0, -90.0, 180.0, 90.0).map(_.asInstanceOf[AnyRef])))
        insertStatement.setString(4, "ready")
        insertStatement.setString(5, """{"viz_type": "OSM"}""")
        insertStatement.executeUpdate()
      }
    } finally {
      resultSet.close()
    }
  }
}