package edu.ucr.cs.bdlab.raptor

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, KryoDataOutput, Output}
import edu.ucr.cs.bdlab.beast.geolite.{ITile, RasterFeature, RasterMetadata}
import edu.ucr.cs.bdlab.beast.sql.RasterMetadataType
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.hadoop.fs.Path
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.test.ScalaSparkTest
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

import java.awt.geom.AffineTransform
import java.io.{ByteArrayInputStream, File}

@RunWith(classOf[JUnitRunner])
class MemoryTileTest extends AnyFunSuite with ScalaSparkTest {
  test("memory tile with integer values") {
    val metadata = new RasterMetadata(0, 0, 100, 100, 100, 100, 4326,
      new AffineTransform())
    val tile: MemoryTile[Int] = new MemoryTile(0, metadata, RasterFeature.create(Array("fileName"),Array("testFile.tif")))
    tile.setPixelValue(0, 0, 100)
    tile.setPixelValue(50, 73, 200)

    assertResult(100)(tile.getPixelValue(0, 0))
    assertResult(200)(tile.getPixelValue(50, 73))
    var pixelCount = 0
    for (y <- tile.y1 to tile.y2; x <- tile.x1 to tile.x2; if tile.isDefined(x, y))
      pixelCount += 1
    assertResult(2)(pixelCount)
  }

  test("memory tile with RGB values") {
    val metadata = new RasterMetadata(0, 0, 100, 100, 100, 100, 4326,
      new AffineTransform())
    val tile: MemoryTile[Array[Byte]] = new MemoryTile(0, metadata, RasterFeature.create(Array("fileName"),Array("testFile.tif")))
    tile.setPixelValue(0, 0, Array[Byte](100, 15, 20))
    tile.setPixelValue(50, 73, Array[Byte](30, 17, 200.toByte))

    assertResult(Array[Byte](100, 15, 20))(tile.getPixelValue(0, 0))
    assertResult(Array[Byte](30, 17, 200.toByte))(tile.getPixelValue(50, 73))
  }

  test("should decompress automatically when reading values") {
    val metadata = new RasterMetadata(0, 0, 100, 100, 100, 100, 4326,
      new AffineTransform())
    val tile: MemoryTile[Array[Byte]] = new MemoryTile(0, metadata, RasterFeature.create(Array("fileName"),Array("testFile.tif")))
    tile.setPixelValue(0, 0, Array[Byte](100, 15, 20))
    tile.setPixelValue(50, 73, Array[Byte](30, 17, 200.toByte))

    tile.compress

    assertResult(Array[Byte](100, 15, 20))(tile.getPixelValue(0, 0))
    assertResult(Array[Byte](30, 17, 200.toByte))(tile.getPixelValue(50, 73))
  }

  test("should decompress automatically when writing values") {
    val metadata = new RasterMetadata(0, 0, 100, 100, 100, 100, 4326,
      new AffineTransform())
    val tile: MemoryTile[Array[Byte]] = new MemoryTile(0, metadata, RasterFeature.create(Array("fileName"),Array("testFile.tif")))
    tile.setPixelValue(0, 0, Array[Byte](100, 15, 20))
    tile.setPixelValue(50, 73, Array[Byte](30, 17, 200.toByte))

    tile.compress
    tile.setPixelValue(tile.x2, tile.y2, Array[Byte](10, 20, 30))
  }

  test("Kryo serialization and deserialization") {
    val metadata = RasterMetadata.create(0, 0, 10, 10, 4326, 10, 10, 10, 10)
    val originalTile = new MemoryTile[Short](0, metadata, RasterFeature.create(Array("fileName"),Array("testFile.tif")))
    originalTile.setPixelValue(0, 0, 15)
    originalTile.setPixelValue(3, 5, 18)
    originalTile.setPixelValue(9, 7, 88)
    val baos = new ByteArrayOutputStream()
    val kryo = new Kryo()
    val kryoOutput = new Output(baos)
    kryo.writeClassAndObject(kryoOutput, originalTile)
    kryoOutput.close()

    val in = new Input(new ByteArrayInputStream(baos.toByteArray))
    val readTile: ITile[Short] = kryo.readClassAndObject(in).asInstanceOf[ITile[Short]]
    for (j <- originalTile.y1 to originalTile.y2; i <- originalTile.x1 to originalTile.x2) {
      assertResult(originalTile.isDefined(i, j), s"Error in defined pixel ($i, $j)")(readTile.isDefined(i, j))
      if (originalTile.isDefined(i, j)) {
        assertResult(originalTile.getPixelValue(i, j), s"Error in pixel value ($i, $j)")(readTile.getPixelValue(i, j))
      }
    }
  }

  test("memory tile with Row Itile") {
    val metadata = new RasterMetadata(0, 0, 100, 100, 100, 100, 4326,
      new AffineTransform())
    val tile: MemoryTile[Int] = new MemoryTile[Int](0, metadata, RasterFeature.create(Array("fileName"),Array("testFile.tif")))
    tile.setPixelValue(0, 0, 100)
    tile.setPixelValue(50, 73, 200)
    assertResult(100)(tile.getPixelValue(0, 0))
    assertResult(200)(tile.getPixelValue(50, 73))
    var pixelCount = 0
    for (y <- tile.y1 to tile.y2; x <- tile.x1 to tile.x2; if tile.isDefined(x, y))
      pixelCount += 1
    assertResult(2)(pixelCount)

  }

  test("Kryo serialization and deserialization with null rasterfeature") {
    val metadata = RasterMetadata.create(0, 0, 10, 10, 4326, 10, 10, 10, 10)
    val originalTile = new MemoryTile[Short](0, metadata)
    originalTile.setPixelValue(0, 0, 15)
    originalTile.setPixelValue(3, 5, 18)
    originalTile.setPixelValue(9, 7, 88)
    val baos = new ByteArrayOutputStream()
    val kryo = new Kryo()
    val kryoOutput = new Output(baos)
    kryo.writeClassAndObject(kryoOutput, originalTile)
    kryoOutput.close()

    val in = new Input(new ByteArrayInputStream(baos.toByteArray))
    val readTile: ITile[Short] = kryo.readClassAndObject(in).asInstanceOf[ITile[Short]]
    for (j <- originalTile.y1 to originalTile.y2; i <- originalTile.x1 to originalTile.x2) {
      assertResult(originalTile.isDefined(i, j), s"Error in defined pixel ($i, $j)")(readTile.isDefined(i, j))
      if (originalTile.isDefined(i, j)) {
        assertResult(originalTile.getPixelValue(i, j), s"Error in pixel value ($i, $j)")(readTile.getPixelValue(i, j))
      }
    }
  }

}

