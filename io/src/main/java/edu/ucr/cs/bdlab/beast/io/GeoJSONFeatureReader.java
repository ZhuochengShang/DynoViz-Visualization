/*
 * Copyright 2018 University of California, Riverside
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucr.cs.bdlab.beast.io;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import edu.ucr.cs.bdlab.beast.common.BeastOptions;
import edu.ucr.cs.bdlab.beast.geolite.EnvelopeND;
import edu.ucr.cs.bdlab.beast.geolite.Feature;
import edu.ucr.cs.bdlab.beast.geolite.IFeature;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SplitCompressionInputStream;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.locationtech.jts.geom.*;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A record reader that reads CSV file with custom field delimiter
 */
@SpatialReaderMetadata(
    description = "Parses a GepJSON file as one record for each feature object",
    shortName = "geojson",
    extension = ".geojson"
)
public class GeoJSONFeatureReader extends FeatureReader {
  private static final Log LOG = LogFactory.getLog(GeoJSONFeatureReader.class);

  /**The mutable feature*/
  protected Feature feature;

  /**An optional attributed to filter the geometries in the input file*/
  private EnvelopeND filterMBR;

  /**The start of the split*/
  protected long start;

  /**The end of the split*/
  protected long end;

  /**The input stream to the raw file (compressed)*/
  protected FSDataInputStream fileIn;

  /**The input stream to the decompressed file*/
  protected InputStream in;

  /**The current position in the file. Either the raw file or the comrpessed file*/
  protected Seekable filePosition;
  protected boolean isCompressedInput;
  protected Decompressor decompressor;

  /**The JSON parser that reads json tokens*/
  private JsonParser jsonParser;

  /**A flag that is raised when end-of-split is reached*/
  protected boolean eos;

  @Override
  public void initialize(InputSplit genericSplit, BeastOptions conf) throws IOException {
    // Open the input split and decompress if necessary
    FileSplit split = (FileSplit) genericSplit;
    this.initialize(split.getPath(), split.getStart(), split.getLength(), conf);
  }

  /**
   * An initializer that can be used outside the regular MapReduce context.
   * @param inputFile the path of the input file
   * @param conf the system configuration
   * @throws IOException if an error happens while opening the input file
   */
  public void initialize(Path inputFile, BeastOptions conf) throws IOException {
    FileStatus fileStatus = inputFile.getFileSystem(conf.loadIntoHadoopConf(null)).getFileStatus(inputFile);
    this.initialize(inputFile, 0, fileStatus.getLen(), conf);
  }

  /**
   * An internal initializer that takes a file path, a start and length.
   * @param file path to the file to open. Works with both compressed and decompressed files (based on extension)
   * @param start the starting offset to parse
   * @param length the number of bytes to parse
   * @param conf the environment configuration
   * @throws IOException if an error happens while opening the input file
   */
  protected void initialize(Path file, long start, long length, BeastOptions conf) throws IOException {
    this.start = start;
    this.eos = length == 0;
    this.end = start + length;

    Configuration hadoopConf = conf.loadIntoHadoopConf(null);

    // open the file and seek to the start of the split
    final FileSystem fs = file.getFileSystem(hadoopConf);
    fileIn = fs.open(file);

    CompressionCodec codec = new CompressionCodecFactory(hadoopConf).getCodec(file);
    if (null != codec) {
      // The file is compressed. Decompress it on the fly
      isCompressedInput = true;
      decompressor = CodecPool.getDecompressor(codec);
      if (codec instanceof SplittableCompressionCodec) {
        // Can decompress a small part of the file
        final SplitCompressionInputStream cIn = ((SplittableCompressionCodec)codec).createInputStream(
            fileIn, decompressor, start, end, SplittableCompressionCodec.READ_MODE.BYBLOCK);
        this.start = cIn.getAdjustedStart();
        this.end = cIn.getAdjustedEnd();
        this.filePosition = cIn;
        this.in = cIn;
      } else {
        // Need to decompress the entire file from the beginning
        this.in = codec.createInputStream(fileIn, decompressor);
        // Read until the end of the file
        this.end = Long.MAX_VALUE;
        this.filePosition = ((CompressionInputStream)this.in);
      }
    } else {
      // Not a compressed file
      fileIn.seek(start);
      filePosition = fileIn;
      in = fileIn;
    }

    JsonFactory jsonFactory = new JsonFactory();
    jsonParser = new SilentJsonParser(jsonFactory.createParser(in));

    // Retrieve the filter MBR
    String filterMBRStr = conf.getString(SpatialFileRDD.FilterMBR());
    if (filterMBRStr != null) {
      String[] parts = filterMBRStr.split(",");
      double[] dblParts = new double[parts.length];
      for (int i = 0; i < parts.length; i++)
        dblParts[i] = Double.parseDouble(parts[i]);
      this.filterMBR = new EnvelopeND(DefaultGeometryFactory, dblParts.length/2, dblParts);
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException {
    // This function reads the next complex feature in the file. A correct spatial feature is an object that contains:
    // - A "type": "Feature" attribute.
    // - A "properties": {} nested object
    // - A nested object with {"type": "geometry", "coordinates": []} schema.
    // Once the object is found, it is returned
    if (eos)
      return false;
    Geometry geometry = null; // Contains the geometry after it is found
    List<String> names = new ArrayList<>(); // Contains the names of the attributes in the "properties"
    List<Object> values = new ArrayList<>(); // Contains the corresponding values in the "properties"
    JsonToken token; // Used to iterate over the file
    // Read until the beginning of the next feature
    boolean featureMarkerFound = false; // A flag that is raised when the "type": "Feature" property is found
    long posOfLastStartObject = this.getFilePosition();
    boolean featureComplete = false;
    while (!featureComplete && (token = jsonParser.nextToken()) != null && posOfLastStartObject < this.end && !eos) {
      if (token == JsonToken.FIELD_NAME) {
        // Read the name first. Do not read the value yet since we do not know the value type.
        String fieldName = jsonParser.getCurrentName();
        if (fieldName.equalsIgnoreCase("type")) {
          String fieldValue = jsonParser.nextTextValue();
          if (fieldValue.equalsIgnoreCase("feature"))
            featureMarkerFound = true;
        } else if (fieldName.equalsIgnoreCase("geometry")) {
          // Found the geometry object, now read it.
          consumeAndCheckToken(jsonParser, JsonToken.START_OBJECT);
          geometry = readGeometry(jsonParser);
        } else if (fieldName.equalsIgnoreCase("properties")) {
          // Found the non-spatial attributes.
          consumeAndCheckToken(jsonParser, JsonToken.START_OBJECT);
          readProperties(names, values);
        } else if (fieldName.equalsIgnoreCase("features")) {
          // The list of features. Just ignore and continue.
        } else {
          // Set additional attributes
          names.add(jsonParser.getCurrentName());
          values.add(jsonParser.nextTextValue());
        }
      } else if (token == JsonToken.START_OBJECT) {
        posOfLastStartObject = this.getFilePosition();
      } else if (token == JsonToken.END_OBJECT) {
        if (featureMarkerFound) {
          // This marks the end of the feature object, break the loop and return whatever was found.
          featureComplete = true;
        } else {
          // Ended an object that is not a feature, reset all read parts to prepare for reading the next feature.
          names.clear();
          values.clear();
          geometry = null;
        }
      } else if (token == JsonToken.NOT_AVAILABLE) {
        eos = true;
      }
    }
    if (!featureMarkerFound) {
      // Finished the file without finding a feature
      eos = true;
      return false;
    }
    // A feature was found, compile it into a feature object and return it.
    feature = Feature.create(geometry, names.toArray(new String[0]), null, values.toArray());
    return true;
  }

  /**
   * Read properties as key-value pairs. It starts by reading the start object, the attributes as key-value pairs, and
   * finally the end object.
   * @param names (out) the names of the properties parsed from the input
   * @param values (out) the values of the properties parsed from the input
   * @throws IOException if an error happens while reading the input
   */
  protected void readProperties(List<String> names, List<Object> values) throws IOException {
    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
      String key = jsonParser.getCurrentName();
      names.add(key);
      JsonToken token = jsonParser.nextToken();
      Object value = parseCurrentValue(jsonParser);
      values.add(value);
    }
  }

  protected Object parseCurrentValue(JsonParser jsonParser) throws IOException {
    JsonToken token = jsonParser.getCurrentToken();
    Object value;
    switch (token) {
      case VALUE_NULL: value = null; break;
      case VALUE_FALSE: value = Boolean.FALSE; break;
      case VALUE_TRUE: value = Boolean.TRUE; break;
      case VALUE_STRING: value = jsonParser.getText(); break;
      case VALUE_NUMBER_INT: value = jsonParser.getLongValue(); break;
      case VALUE_NUMBER_FLOAT: value = jsonParser.getDoubleValue(); break;
      case START_ARRAY:
        // Read an array of values. For simplicity, we assume an array of primitive and similar type.
        scala.collection.mutable.ArrayBuffer<Object> listValues = new scala.collection.mutable.ArrayBuffer<>();
        while (jsonParser.nextToken() != JsonToken.END_ARRAY)
          listValues.$plus$eq$colon(parseCurrentValue(jsonParser));
        value = listValues.toSeq();
        break;
      case START_OBJECT:
        // Read a nested object as a map
        List<String> mapNames = new ArrayList<>();
        List<Object> mapValues = new ArrayList<>();
        readProperties(mapNames, mapValues);
        scala.collection.immutable.HashMap<String, Object> mapValue = new scala.collection.immutable.HashMap<>();
        for (int i = 0; i < mapNames.size(); i++)
          mapValue = mapValue.$plus(new Tuple2<>(mapNames.get(i), mapValues.get(i)));
        value = mapValue;
        break;
      default:
        throw new RuntimeException(String.format("Unsupported value type '%s'", token));
    }
    return value;
  }

  /**
   * Parses a GeoJSON geometry from the given parser.
   * This function assumes that the next token is the start object of the geometry attribute.
   * @param jsonParser the JsonParser to read tokens from
   * @return the parsed geometry
   * @throws IOException if an error occurs while reading from the JSON Parser.
   */
  public static Geometry parseGeometry(JsonParser jsonParser) throws IOException {
    consumeAndCheckToken(jsonParser, JsonToken.START_OBJECT);
    return readGeometry(jsonParser);
  }

  /**
   * Parses a list of coordinates. This function assumes that the START_ARRAY token has already been consumed
   * and it consumes all values until it reaches and consumes the END_ARRAY token.
   * @param jsonParser
   * @return
   * @throws IOException
   */
  protected static Object parseCoordinates(CoordinateSequenceFactory csFactory, JsonParser jsonParser) throws IOException {
    JsonToken token = jsonParser.nextToken();
    switch (token) {
      case VALUE_NUMBER_FLOAT:
      case VALUE_NUMBER_INT:
        ArrayList<Double> coords = new ArrayList<>(3);
        while (token != JsonToken.END_ARRAY) {
          coords.add(jsonParser.getDoubleValue());
          token = jsonParser.nextToken();
        }
        double[] retVal = new double[coords.size()];
        for (int i = 0; i < coords.size(); i++)
          retVal[i] = coords.get(i);
        return retVal;
      case START_ARRAY:
        // Array within array indicates a coordinate sequence or an array of coordinate sequences
        ArrayList<Object> retVal2 = new ArrayList<>();
        while (token != JsonToken.END_ARRAY) {
          switch (token) {
            case START_ARRAY:
              retVal2.add(parseCoordinates(csFactory, jsonParser));
              break;
            case END_ARRAY:
              break;
            default:
              throw new RuntimeException("Unexpected token "+token+" within a coordinates list");
          }
          token = jsonParser.nextToken();
        }
        return retVal2.toArray();
      case END_ARRAY:
        // This indicates an empty list
        return null;
      default:
        throw new RuntimeException("Unexpected token: "+token);
    }
  }

  /**
   * Parses an array of array of doubles double[][] into a coordinate sequence.
   * @param csFactory the coordinate sequence factory to use for creating the coordinate sequence
   * @param coordList the value to parse
   * @return the created coordinate sequence
   */
  protected static CoordinateSequence parseCoordinateSequence(CoordinateSequenceFactory csFactory, Object[] coordList) {
    CoordinateSequence cs = csFactory.create(coordList.length, ((double[])coordList[0]).length, 0);
    for (int iCoord = 0; iCoord < coordList.length; iCoord++) {
      double[] pointCoords = (double[]) coordList[iCoord];
      for (int iVal = 0; iVal < pointCoords.length; iVal++)
        cs.setOrdinate(iCoord, iVal, pointCoords[iVal]);
    }
    return cs;
  }

  /**
   * Reads a geometry object from the JsonParser. The assumption is that the start object token of the geometry has
   * already been consumed. This function should read and consume the end object of the geometry
   * @return the given geometry if it was reused or a new geometry object otherwise.
   * @throws IOException if an error happens while reading the input
   */
  protected static Geometry readGeometry(JsonParser jsonParser) throws IOException {
    String geometryType = null;
    Object coordinatesVal = null;
    JsonToken token = jsonParser.nextToken();
    while (token != JsonToken.END_OBJECT) {
      switch (token) {
        case FIELD_NAME:
          String fieldName = jsonParser.getCurrentName();
          switch (fieldName.toLowerCase()) {
            case "type":
              geometryType = jsonParser.nextTextValue();
              break;
            case "coordinates":
              consumeAndCheckToken(jsonParser, JsonToken.START_ARRAY);
              coordinatesVal = parseCoordinates(DefaultGeometryFactory.getCoordinateSequenceFactory(), jsonParser);
              break;
            case "geometries":
              consumeAndCheckToken(jsonParser, JsonToken.START_ARRAY);
              ArrayList<Geometry> geometries = new ArrayList<>();
              token = jsonParser.nextToken();
              while (token != JsonToken.END_ARRAY) {
                switch (token) {
                  case START_OBJECT:
                    geometries.add(readGeometry(jsonParser));
                    break;
                  case END_ARRAY:
                    break;
                  default:
                    throw new RuntimeException("Unexpected token "+token);
                }
                token = jsonParser.nextToken();
              }
              coordinatesVal = geometries;
              break;
            default:
              throw new RuntimeException("Unexpected attribute "+fieldName);
          }
          break;
        default:
          throw new RuntimeException("Unexpected token type: "+token);
      }
      token = jsonParser.nextToken();
    }

    if (geometryType == null || coordinatesVal == null)
      return DefaultGeometryFactory.createEmpty(2);
    Object[] coordList;
    CoordinateSequence cs;
    LinearRing[] rings;
    switch (geometryType.toLowerCase()) {
      case "point":
        // http://wiki.geojson.org/GeoJSON_draft_version_6#Point
        if (!(coordinatesVal instanceof double[]))
          throw new RuntimeException("Unexpected coordinates for point geometry");
        double[] coords = (double[]) coordinatesVal;
        Coordinate pointCoord = new Coordinate();
        pointCoord.setX(coords[0]);
        pointCoord.setY(coords[1]);
        if (coords.length > 2)
          pointCoord.setZ(coords[2]);
        if (coords.length > 3)
          pointCoord.setM(coords[3]);
        return DefaultGeometryFactory.createPoint(pointCoord);
      case "linestring":
        // http://wiki.geojson.org/GeoJSON_draft_version_6#LineString
        if (!(coordinatesVal instanceof Object[]))
          throw new RuntimeException("Unexpected coordinates for line string geometry");
        coordList = (Object[]) coordinatesVal;
        if (coordList.length == 0)
          DefaultGeometryFactory.createLineString();
        cs = parseCoordinateSequence(DefaultGeometryFactory.getCoordinateSequenceFactory(), coordList);
        return DefaultGeometryFactory.createLineString(cs);
      case "polygon":
        // http://wiki.geojson.org/GeoJSON_draft_version_6#Polygon
        if (!(coordinatesVal instanceof Object[]))
          throw new RuntimeException("Unexpected coordinates for polygon geometry");
        coordList = (Object[]) coordinatesVal;
        rings = new LinearRing[coordList.length];
        for (int iRing = 0; iRing < rings.length; iRing++) {
          cs = parseCoordinateSequence(DefaultGeometryFactory.getCoordinateSequenceFactory(),
              (Object[])(coordList[iRing]));
          rings[iRing] = DefaultGeometryFactory.createLinearRing(cs);
        }
        return DefaultGeometryFactory.createPolygon(rings[0], Arrays.copyOfRange(rings, 1, rings.length));
      case "multipoint":
        // http://wiki.geojson.org/GeoJSON_draft_version_6#MultiPoint
        if (!(coordinatesVal instanceof Object[]))
          throw new RuntimeException("Unexpected coordinates for multipoint geometry");
        coordList = (Object[]) coordinatesVal;
        cs = parseCoordinateSequence(DefaultGeometryFactory.getCoordinateSequenceFactory(), coordList);
        return DefaultGeometryFactory.createMultiPoint(cs);
      case "multilinestring":
        // http://wiki.geojson.org/GeoJSON_draft_version_6#MultiLineString
        if (!(coordinatesVal instanceof Object[]))
          throw new RuntimeException("Unexpected coordinates for multi line string geometry");
        coordList = (Object[]) coordinatesVal;
        LineString[] lineStrings = new LineString[coordList.length];
        for (int iLineString = 0; iLineString < lineStrings.length; iLineString++) {
          cs = parseCoordinateSequence(DefaultGeometryFactory.getCoordinateSequenceFactory(),
              (Object[])(coordList[iLineString]));
          lineStrings[iLineString] = DefaultGeometryFactory.createLineString(cs);
        }
        return DefaultGeometryFactory.createMultiLineString(lineStrings);
      case "multipolygon":
        // http://wiki.geojson.org/GeoJSON_draft_version_6#MultiPolygon
        if (!(coordinatesVal instanceof Object[]))
          throw new RuntimeException("Unexpected coordinates for multi polygon geometry");
        coordList = (Object[]) coordinatesVal;
        Polygon[] polygons = new Polygon[coordList.length];
        for (int iPolygon = 0; iPolygon < polygons.length; iPolygon++) {
          Object[] subCoordList = (Object[]) coordList[iPolygon];
          rings = new LinearRing[subCoordList.length];
          for (int iRing = 0; iRing < rings.length; iRing++) {
            cs = parseCoordinateSequence(DefaultGeometryFactory.getCoordinateSequenceFactory(),
                (Object[])(subCoordList[iRing]));
            rings[iRing] = DefaultGeometryFactory.createLinearRing(cs);
          }
          polygons[iPolygon] = DefaultGeometryFactory.createPolygon(rings[0], Arrays.copyOfRange(rings, 1, rings.length));
        }
        return DefaultGeometryFactory.createMultiPolygon(polygons);
    case "geometrycollection":
      // http://wiki.geojson.org/GeoJSON_draft_version_6#GeometryCollection
      List<Geometry> geoms = (List<Geometry>) coordinatesVal;
      return DefaultGeometryFactory.createGeometryCollection(geoms.toArray(new Geometry[0]));
    default:
      throw new RuntimeException(String.format("Unexpected geometry type '%s'", geometryType));
    }
  }

  /**
   * Read the next token and ensure it is a numeric token. Either Integer or Float
   */
  private static void consumeNumber(JsonParser jsonParser) throws IOException {
    JsonToken actual = jsonParser.nextToken();
    if (actual != JsonToken.VALUE_NUMBER_FLOAT && actual != JsonToken.VALUE_NUMBER_INT) {
      // Throw a parse exception.
      // TODO use a specialized exception(s)
      int lineNumber = jsonParser.getTokenLocation().getLineNr();
      int characterNumber = jsonParser.getTokenLocation().getColumnNr();
      throw new RuntimeException(String.format("Error parsing GeoJSON file. " +
          "Expected numeric value but found %s at line %d character %d", actual, lineNumber, characterNumber));
    }
  }

  private static void consumeAndCheckToken(JsonParser jsonParser, JsonToken expected) throws IOException {
    JsonToken actual = jsonParser.nextToken();
    if (actual != expected) {
      // Throw a parse exception.
      // TODO use a specialized exception(s)
      int lineNumber = jsonParser.getTokenLocation().getLineNr();
      int characterNumber = jsonParser.getTokenLocation().getColumnNr();
      throw new RuntimeException(String.format("Error parsing GeoJSON file. " +
          "Expected token %s but found %s at line %d character %d", expected, actual, lineNumber, characterNumber));
    }
  }

  private static void consumeAndCheckFieldName(JsonParser jsonParser, String expected) throws IOException {
    consumeAndCheckToken(jsonParser, JsonToken.FIELD_NAME);
    String actual = jsonParser.getCurrentName();
    if (!expected.equalsIgnoreCase(actual)) {
      // Throw a parse exception.
      // TODO use a specialized exception(s)
      int lineNumber = jsonParser.getTokenLocation().getLineNr();
      int characterNumber = jsonParser.getTokenLocation().getColumnNr();
      throw new RuntimeException(String.format("Error parsing GeoJSON file. " +
          "Expected field '%s' but found '%s' at line %d character %d", expected, actual, lineNumber, characterNumber));
    }
  }

  @Override
  public IFeature getCurrentValue() {
    return feature;
  }

  private long getFilePosition() throws IOException {
    long retVal;
    if (isCompressedInput && null != filePosition) {
      retVal = filePosition.getPos();
    } else {
      retVal = start + jsonParser.getTokenLocation().getByteOffset();
    }
    return retVal;
  }

  @Override
  public float getProgress() throws IOException {
    if (start == end) {
      return 0.0f;
    } else {
      return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
    }
  }

  @Override
  public void close() throws IOException {
    jsonParser.close();
  }

}
