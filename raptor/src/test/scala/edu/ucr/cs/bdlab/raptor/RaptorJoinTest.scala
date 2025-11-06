package edu.ucr.cs.bdlab.raptor

import edu.ucr.cs.bdlab.beast.cg.Reprojector
import edu.ucr.cs.bdlab.beast.cg.SpatialDataTypes.RasterRDD
import edu.ucr.cs.bdlab.beast.common.BeastOptions
import edu.ucr.cs.bdlab.beast.geolite.{Feature, IFeature, RasterFeature, RasterMetadata}
import edu.ucr.cs.bdlab.beast.io.ReadWriteMixin.ReadWriteMixinFunctions
import edu.ucr.cs.bdlab.raptor.RaptorMixin.RasterReadMixinFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.locationtech.jts.geom.{CoordinateSequence, Envelope, GeometryFactory, PrecisionModel}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.awt.geom.Point2D

@RunWith(classOf[JUnitRunner])
class RaptorJoinTest extends AnyFunSuite with ScalaSparkTest {

  val factory = new GeometryFactory(new PrecisionModel(PrecisionModel.FLOATING), 4326)
  def createSequence(points: (Double, Double)*): CoordinateSequence = {
    val cs = factory.getCoordinateSequenceFactory.create(points.length, 2)
    for (i <- points.indices) {
      cs.setOrdinate(i, 0, points(i)._1)
      cs.setOrdinate(i, 1, points(i)._2)
    }
    cs
  }

  test("RaptorJoinZS with RDD") {
    val rasterFile = makeFileCopy("/raptor/glc2000_small.tif").getPath
    val testPoly = factory.toGeometry(new Envelope(-82.76, -80.25, 31.91, 35.17))
    val vector: RDD[(Long, IFeature)] = sparkContext.parallelize(Seq((1L, Feature.create(null, testPoly))))
    val raster: RasterFileRDD[Int] = new RasterFileRDD[Int](sparkContext, rasterFile, new BeastOptions())

    val values: RDD[RaptorJoinResult[Int]] = RaptorJoin.raptorJoinIDFull(raster, vector, new BeastOptions())

    // Values sorted by (x, y)
    val finalValues: Array[RaptorJoinResult[Int]] = values.collect().sortWith((a, b) => a.x < b.x || a.x == b.x && a.y < b.y)
    assert(finalValues.length == 6)
    assert(finalValues(0).x == 69)
    assert(finalValues(0).y == 48)
  }

  test("RaptorJoin with multiband") {
    val rasterFile = makeFileCopy("/rasters/FRClouds.tif").getPath
    val testPoly = factory.toGeometry(new Envelope(0, 3, 45, 47))
    val vector: RDD[(Long, IFeature)] = sparkContext.parallelize(Seq((1L, Feature.create(null, testPoly))))
    val raster: RasterFileRDD[Array[Int]] = new RasterFileRDD[Array[Int]](sparkContext, rasterFile, IRasterReader.OverrideSRID -> 4326)

    val values: RDD[RaptorJoinResult[Array[Int]]] = RaptorJoin.raptorJoinIDFull(raster, vector, new BeastOptions())

    assertResult(classOf[Array[Int]])(values.first().m.getClass)
  }

  test("RaptorJoinZS with RDD and reproject") {
    val rasterFile = makeFileCopy("/rasters/glc2000_small_EPSG3857.tif").getPath
    val testPoly = factory.toGeometry(new Envelope(-109, -106, 36, 40))
    val vector: RDD[(Long, IFeature)] = sparkContext.parallelize(Seq((1L, Feature.create(null, testPoly))))
    val raster: RasterFileRDD[Int] = new RasterFileRDD[Int](sparkContext, rasterFile, new BeastOptions())

    val values: RDD[RaptorJoinResult[Int]] = RaptorJoin.raptorJoinIDFull(raster, vector, new BeastOptions())

    // Values sorted by (x, y)
    val finalValues: Array[RaptorJoinResult[Int]] = values.collect().sortWith((a, b) => a.x < b.x || a.x == b.x && a.y < b.y)
    assert(finalValues.length == 6)
    assert(finalValues(0).m == 12.0f)
  }

  test("EmptyResult with RDD") {
    val rasterFile = makeFileCopy("/raptor/glc2000_small.tif").getPath
    val testPoly = factory.createPolygon()
    val vector: RDD[(Long, IFeature)] = sparkContext.parallelize(Seq((1L, Feature.create(null, testPoly))))
    val raster: RasterFileRDD[Int] = new RasterFileRDD[Int](sparkContext, rasterFile, new BeastOptions())

    val values: RDD[RaptorJoinResult[Int]] = RaptorJoin.raptorJoinIDFull(raster, vector, new BeastOptions())

    // Values sorted by (x, y)
    val finalValues: Array[RaptorJoinResult[Int]] = values.collect().sortWith((a, b) => a.x < b.x || a.x == b.x && a.y < b.y)
    assert(finalValues.isEmpty)
  }

  test("Join Small Area RDD") {
    // AOI
    val Yuma = "/Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/originaldata/yuma/SubCheckArea.shp"
    val AOI = sparkContext.shapefile(Yuma)
    val mbr = AOI.first().getGeometry //.getEnvelopeInternal

    val Landsat_fileB4 = "/Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/originaldata/LC08_L1TP_038037_20190829_20190903_01_T1_B4.tif"
    val raster: RasterRDD[Int] = sparkContext.geoTiff(Landsat_fileB4)

    val values: RDD[RaptorJoinFeature[Int]] = RaptorJoin.raptorJoinFeature(raster, AOI, new BeastOptions())
    values.collect().sortWith((a, b) => a.x < b.x || a.x == b.x && a.y < b.y)

    val metadata = values.first().rasterMetadata
    val pixelSeq: RDD[(Double, Double, Int)] = values.map(pixel => {
      val point: Point2D.Double = new Point2D.Double
      metadata.gridToModel(pixel.x, pixel.y, point)
      //println(pixel.x,pixel.y,point.x,point.y,pixel.m)
      (point.x, point.y, pixel.m)
    })

    val geom = Reprojector.reprojectGeometry(mbr, metadata.srid)
    val lineMBR: Envelope = geom.getEnvelopeInternal
    println(lineMBR)
    // recalculate Raster w, h using pixel size
    val cellSizeX = metadata.getPixelScaleX
    val epsilon = 1e-10
    val rasterW = Math.round((Math.abs(lineMBR.getMaxX - lineMBR.getMinX) / cellSizeX) + epsilon).toInt
    val rasterH = Math.round((Math.abs(lineMBR.getMaxY - lineMBR.getMinY) / cellSizeX) + epsilon).toInt

    //val targetMetadata = RasterMetadata.create(x1, y2, x2, y1, metadata.srid, rasterW, rasterH, 256, 256)
    val targetMetadata = RasterMetadata.create(lineMBR.getMinX, lineMBR.getMaxY, lineMBR.getMaxX, lineMBR.getMinY, metadata.srid, rasterW, rasterH, 256, 256)
    val res = RasterOperationsGlobal.rasterizePoints(pixelSeq,targetMetadata,RasterFeature.create(Array("fileName"), Array("ndvi.tif")))
//    res.foreach(tile => {
//      for (y <- tile.y1 to tile.y2) {
//        for (x <- tile.x1 to tile.x2) {
//          //if (tile.isDefined(x, y))
//          val point: Point2D.Double = new Point2D.Double
//          targetMetadata.gridToModel(x,y,point)
//          println("define: ", x, y, point.x, point.y, tile.isDefined(x, y))
//          val array = tile.getPixelValue(x, y)
//        }
//      }
//    })

    val output = "/Users/clockorangezoe/Desktop/BAITSSS_project/testfolder/aoidata_rdpro"
    GeoTiffWriter.saveAsGeoTiff(res, output + "/ndvi.value2.tif", Seq(GeoTiffWriter.WriteMode -> "compatibility", GeoTiffWriter.FillValue -> 0)) //GeoTiffWriter.BigTiff -> "yes"

  }
}
