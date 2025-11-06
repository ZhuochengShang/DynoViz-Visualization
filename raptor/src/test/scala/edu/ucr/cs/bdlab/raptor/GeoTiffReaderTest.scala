package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.Reprojector.{TransformationInfo, findTransformationInfo}
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.RasterRDD
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{RasterFeature, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.ReadWriteMixin.ReadWriteMixinFunctions
import edu.ucr.cs.bdlab.raptor.RaptorMixin.RasterReadMixinFunctions
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.beast.CRSServer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.test.ScalaSparkTest
import org.geotools.coverage.grid.GridCoverage2D
import org.geotools.coverage.grid.io.GridFormatFinder
import org.geotools.referencing.CRS
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{Envelope, Geometry}
import org.opengis.parameter.GeneralParameterValue
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.awt.geom.Point2D
import java.io.File

@RunWith(classOf[JUnitRunner])
class GeoTiffReaderTest extends AnyFunSuite with ScalaSparkTest {

  test("Read Stripped file") {
    val rasterPath = new Path(makeFileCopy("/rasters/FRClouds.tif").getPath)
    val fileSystem = rasterPath.getFileSystem(new Configuration())
    val reader = new GeoTiffReader[Array[Int]]
    try {
      reader.initialize(fileSystem, rasterPath.toString, "0", new BeastOptions())
      assert(reader.metadata.rasterWidth == 99)
      assert(reader.metadata.rasterHeight == 72)
      assert(reader.metadata.getPixelScaleX == 0.17578125)
      // Test conversion
      // Transform origin point from raster to vector
      val outPoint = new Point2D.Double
      reader.metadata.gridToModel(0, 0, outPoint)
      assert(-6.679688 - outPoint.x < 1E-3)
      assert(53.613281 - outPoint.y < 1E-3)
      // Transform the other corner point
      reader.metadata.gridToModel(reader.metadata.rasterWidth, reader.metadata.rasterHeight, outPoint)
      assert(10.7226 - outPoint.x < 1E-3)
      assert(40.957 - outPoint.y < 1E-3)
      // Test the inverse transformation on one corner
      reader.metadata.modelToGrid(-6.679688, 53.613281, outPoint)
      assert(outPoint.getX.toInt == 0)
      assert(outPoint.getY.toInt == 0)
      // Test the inverse transformation
      reader.metadata.modelToGrid(-0.06, 49.28, outPoint)
      assert(outPoint.getX.toInt == 37)
      assert(outPoint.getY.toInt == 24)
      val tileID = reader.metadata.getTileIDAtPixel(37, 24)
      val tile = reader.readTile(tileID)
      val pixel = tile.getPixelValue(37, 24)
      assertResult(Array(0x45, 0x9c, 0x8b))(pixel)
    } finally {
      reader.close()
    }
  }

  test("Read tiled GeoTIFF") {
    val rasterPath = new Path(makeFileCopy("/rasters/glc2000_small.tif").getPath)
    val fileSystem = rasterPath.getFileSystem(new Configuration())
    val reader = new GeoTiffReader[Int]
    try {
      reader.initialize(fileSystem, rasterPath.toString, "0", new BeastOptions)
      assert(256 == reader.metadata.rasterWidth)
      assert(128 == reader.metadata.rasterHeight)

      val tile1 = reader.readTile(reader.metadata.getTileIDAtPoint(23.224, 32.415))
      val tile2 = reader.readTile(reader.metadata.getTileIDAtPoint(33.694, 14.761))
      assertResult(8)(tile1.getPointValue(23.224, 32.415))
      assertResult(22)(tile2.getPointValue(33.694, 14.761))
    } finally {
      reader.close()
    }
  }

  test("Read banded GeoTIFF") {
    val rasterPath = new Path(makeFileCopy("/rasters/glc2000_banded_small.tif").getPath)
    val fileSystem = rasterPath.getFileSystem(new Configuration())
    val reader = new GeoTiffReader[Array[Float]]
    try {
      reader.initialize(fileSystem, rasterPath.toString, "0", new BeastOptions)
      assert(256 == reader.metadata.rasterWidth)
      assert(128 == reader.metadata.rasterHeight)
      val tile1 = reader.readTile(reader.metadata.getTileIDAtPoint(31.277, 26.954))
      assertArrayEquals(Array[Float](880664.8f, 16.0f), tile1.getPointValue(31.277, 26.954), 1E-3F)
    } finally {
      reader.close()
    }
  }

  test("Read projected GeoTIFF file") {
    val rasterPath = new Path(makeFileCopy("/rasters/glc2000_small_EPSG3857.tif").getPath)
    val fileSystem = rasterPath.getFileSystem(new Configuration())
    val reader = new GeoTiffReader
    try {
      reader.initialize(fileSystem, rasterPath.toString, "0", new BeastOptions)
      assert(5 == reader.metadata.rasterWidth)
      assert(4 == reader.metadata.rasterHeight)
      assert(3857 == reader.metadata.srid)
    } finally {
      reader.close()
    }
  }

  test("Read projected GeoTIFF Landsat") {
    val rasterPath = new Path(locateResource("/rasters/sample.tif").getPath)
    val fileSystem = rasterPath.getFileSystem(new Configuration())
    val reader = new GeoTiffReader
    try {
      reader.initialize(fileSystem, rasterPath.toString, "0", new BeastOptions())
      val srid = reader.metadata.srid
      assert(srid != 0)

      //GeoTools
      val format = GridFormatFinder.findFormat(new File(rasterPath.toString))
      val Greader = format.getReader(new File(rasterPath.toString))
      val coverage: GridCoverage2D = Greader.read(Array[GeneralParameterValue]())
      val crsGeotools = coverage.getCoordinateReferenceSystem
      val sridGeotools = CRSServer.crsToSRID(crsGeotools)

      assert(sridGeotools == srid)
    } finally {
      reader.close()
    }
  }

  test("Use default planar configuration if not set") {
    val rasterPath = new Path(locateResource("/rasters/glc2000_small_noplanar.tif").getPath)
    val fileSystem = rasterPath.getFileSystem(new Configuration())
    val reader = new GeoTiffReader
    try {
      reader.initialize(fileSystem, rasterPath.toString, "0", new BeastOptions())
      val tile = reader.readTile(0)
      assertResult(20)(tile.getPixelValue(5, 0))
    } finally {
      reader.close()
    }
  }

  test("Read a file with some incomplete tiles") {
    val rasterPath = new Path(locateResource("/rasters/FRClouds.tif").getPath)
    val fileSystem = rasterPath.getFileSystem(new Configuration())
    val reader = new GeoTiffReader
    try {
      reader.initialize(fileSystem, rasterPath.toString, "0", new BeastOptions())
      for (tileID <- reader.metadata.tileIDs) {
        val tile = reader.readTile(tileID)
        for ((x, y) <- tile.pixelLocations)
          tile.isDefined(x, y)
      }

    } finally {
      reader.close()
    }
  }

  test("Read a file a separate mask layer") {
    val rasterPath = new Path(locateResource("/rasters/masked.tif").getPath)
    val fileSystem = rasterPath.getFileSystem(new Configuration())
    val reader = new GeoTiffReader
    try {
      reader.initialize(fileSystem, rasterPath.toString, "0", new BeastOptions())
      val tile = reader.readTile(0)
      assertResult(32)(tile.tileWidth)
      assert(!tile.isDefined(0, 0))
      assert(tile.isDefined(6, 0))
      assert(!tile.isDefined(9, 0))
    } finally {
      reader.close()
    }
  }

  test("Read tiled Big GeoTiff Big Endian") {
    val rasterPath = new Path(locateResource("/rasters/glc2000_bigtiff2.tif").getPath)
    val fileSystem = rasterPath.getFileSystem(new Configuration())
    val reader = new GeoTiffReader[Int]
    try {
      reader.initialize(fileSystem, rasterPath.toString, "0", new BeastOptions)
      assert(256 == reader.metadata.rasterWidth)
      assert(128 == reader.metadata.rasterHeight)
      reader.readTile(0)
    } finally {
      reader.close()
    }
  }

  test("Override fill value") {
    val rasterPath = locateResource("/rasters/glc2000_small.tif")
    val fileSystem = new Path(rasterPath.getPath).getFileSystem(new Configuration())
    val reader = new GeoTiffReader[Int]
    try {
      reader.initialize(fileSystem, rasterPath.getPath, "0", "fillvalue" -> 8)
      val tile1 = reader.readTile(reader.metadata.getTileIDAtPoint(23.224, 32.415))
      assert(tile1.isEmptyAt(23.224, 32.415))
    } finally {
      reader.close()
    }
  }

  test("Read GeoTIFF with 64-bit numbers") {
    val rasterPath = new Path(makeFileCopy("/rasters/NLDAS-64.tif").getPath)
    val fileSystem = rasterPath.getFileSystem(new Configuration())
    val reader = new GeoTiffReader[Array[Double]]
    try {
      reader.initialize(fileSystem, rasterPath.toString, "0", new BeastOptions)
      val tile = reader.readTile(0)
      assertResult(DoubleType)(tile.componentType)
      assertResult(11)(tile.numComponents)
      var pixelValue: Array[Double] = tile.getPixelValue(0, 0)
      assert((pixelValue(0) - -0.96).abs < 1E-3)
      assert((pixelValue(5) - 261.43).abs < 1E-3)
      pixelValue = tile.getPixelValue(1, 2)
      assert((pixelValue(0) - -1.51).abs < 1E-3)
      assert((pixelValue(4) - 5.36).abs < 1E-3)
    } finally {
      reader.close()
    }
  }

  test("Read GeoTiff with Datetime") {
    val Planet_RDD: RasterRDD[Array[Int]] = sparkContext.geoTiff("/Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/originaldata/Yuma_09_4B/2019-09-01")
    val PlanetFloat: RasterRDD[Float] = RasterOperationsLocal.mapPixels(Planet_RDD, (p:Array[Int])=>{
      val value = ((p(1) - p(0)).toFloat / (p(1) + p(0)).toFloat).toFloat
      var pixelValue = value
      if (value > 1) {
        pixelValue = 1
      } else if (value < -1) {
        pixelValue = -1
      }
      pixelValue
    })
    val targetCRS = CRS.decode("EPSG:4326") //4326, 3857
    println(" Start to collect metadata : ")
    val allMetadata: Array[RasterMetadata] = RasterMetadata.allMetadata(PlanetFloat)

    val initialMetadata = PlanetFloat.first().rasterMetadata
    println("Initial metadata: " + initialMetadata)
    var minX1 = Double.MaxValue
    var minY1 = Double.MaxValue

    var maxX2 = Double.MinValue
    var maxY2 = Double.MinValue

    var minCellsizeX = Double.MaxValue
    var minCellsizeY = Double.MaxValue

    var rasterWidth = 0
    var rasterHeight = 0
    allMetadata.foreach(originMetadata => {
      val sourceCRS = CRSServer.sridToCRS(originMetadata.srid)

      val x1 = originMetadata.x1
      val y1 = originMetadata.y1
      val x2 = originMetadata.x2
      val y2 = originMetadata.y2
      val corners = Array[Double](x1, y1,
        x2, y1,
        x2, y2,
        x1, y2)
      originMetadata.g2m.transform(corners, 0, corners, 0, 4)

      val transform: TransformationInfo = findTransformationInfo(sourceCRS, targetCRS)
      transform.mathTransform.transform(corners, 0, corners, 0, 4)

      minX1 = minX1 min corners(0) min corners(2) min corners(4) min corners(6)
      maxX2 = maxX2 max corners(0) max corners(2) max corners(4) max corners(6)
      minY1 = minY1 min corners(1) min corners(3) min corners(5) min corners(7)
      maxY2 = maxY2 max corners(1) max corners(3) max corners(5) max corners(7)

      val col = originMetadata.rasterWidth
      val row = originMetadata.rasterHeight

      minCellsizeX = minCellsizeX min ((maxX2 - minX1).abs / col)
      minCellsizeY = minCellsizeY min ((maxY2 - minY1).abs / row)
    })

    rasterWidth = Math.floor(Math.abs(maxX2 - minX1) / minCellsizeX).toInt
    rasterHeight = Math.floor(Math.abs(maxY2 - minY1) / minCellsizeY).toInt

    val targetMetadata = RasterMetadata.create(minX1, maxY2, maxX2, minY1, 4326, rasterWidth, rasterHeight, 256, 256)
    val PlanetReshaped = RasterOperationsFocal.reshapeNN(PlanetFloat, RasterMetadata=>targetMetadata)
    println(targetMetadata)

    //(j - y1) * this.tileWidth + (i - x1)
//    PlanetReshaped.foreach(tile=>{
//      val inputTile: MemoryTile[Array[Int]] = new MemoryTile(tile.tileID, targetMetadata, RasterFeature.create(Array("fileName"),Array("testFile.tif")))
//      for(x <- tile.x1 to tile.x2){
//        for (y <- tile.y1 to tile.y2) {
//          val v = tile.getPixelValue(x,y)
//          inputTile.setPixelValue(x,y,v)
//          inputTile.getPixelValue(x,y)
//        }
//      }
//    })
// AOI
    val Yuma = "/Users/clockorangezoe/Documents/phd_projects/code/pyMETRIC/yuma2019/study_area/YumaAgMain_latlon2.shp"
    val AOI = sparkContext.shapefile(Yuma)

    val NDVI_Join: RDD[RaptorJoinFeature[Float]] = RaptorJoin.raptorJoinFeature(PlanetReshaped, AOI) //.map(t => (timestamp, t))
    val NDVI_rasterize: RasterRDD[Float] = RasterizeJoinResult(NDVI_Join, AOI.first().getGeometry, PlanetFloat.first().rasterMetadata) //.map(t => (timestamp, t))

    val output = "/Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/aoidata_rdpro"
    GeoTiffWriter.saveAsGeoTiff(NDVI_rasterize, output + "/planet.ndvi.valueR2.tif", Seq(GeoTiffWriter.WriteMode -> "compatibility", GeoTiffWriter.FillValue -> 0)) //GeoTiffWriter.BigTiff -> "yes"
  }

  def RasterizeJoinResult(raptorJoinResult: RDD[RaptorJoinFeature[Float]], MBR: Geometry, rasterM: RasterMetadata): RasterRDD[Float] = {
    // reproject AOI to ratser crs
    val mbr = MBR //.getEnvelopeInternal
    val metadata = rasterM
    val pixelSeq: RDD[(Double, Double, Float)] = raptorJoinResult.map(pixel => {
      val point: Point2D.Double = new Point2D.Double
      metadata.gridToModel(pixel.x, pixel.y, point)
      (point.x, point.y, pixel.m)
    })

    val geom = Reprojector.reprojectGeometry(mbr, metadata.srid)

    val corner1: Point2D.Double = new Point2D.Double
    val corner2: Point2D.Double = new Point2D.Double

    val lineMBR: Envelope = geom.getEnvelopeInternal
    metadata.modelToGrid(lineMBR.getMinX, lineMBR.getMinY, corner1)
    metadata.modelToGrid(lineMBR.getMaxX, lineMBR.getMaxY, corner2)

    val i1 = Math.max(0, Math.min(corner1.x, corner2.x))
    val i2 = Math.min(metadata.rasterWidth, Math.ceil(Math.max(corner1.x, corner2.x)))
    val j1 = Math.max(0, Math.min(corner1.y, corner2.y))
    val j2 = Math.min(metadata.rasterHeight, Math.ceil(Math.max(corner1.y, corner2.y)))

    metadata.gridToModel(i1, j1, corner1)
    metadata.gridToModel(i2, j2, corner2)
    val x1 = Math.min(corner1.x, corner2.x)
    val y1 = Math.min(corner1.y, corner2.y)
    val x2 = Math.max(corner1.x, corner2.x)
    val y2 = Math.max(corner1.y, corner2.y)

    // recalculate Raster w, h using pixel size
    val cellSizeX = metadata.getPixelScaleX
    val rasterW = Math.floor((x2 - x1) / cellSizeX).toInt
    val rasterH = Math.floor((y2 - y1) / cellSizeX).toInt

    val targetMetadata = RasterMetadata.create(lineMBR.getMinX, lineMBR.getMaxY, lineMBR.getMaxX, lineMBR.getMinY, metadata.srid, rasterW, rasterH, rasterW, rasterH)

    val landsatRaster = RasterOperationsGlobal.rasterizePoints(pixelSeq, targetMetadata, RasterFeature.create(Array("fileName"), Array("landsat.tif")))
    (landsatRaster)

  }
}
