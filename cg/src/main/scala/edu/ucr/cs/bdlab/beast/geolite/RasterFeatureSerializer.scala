/*
 * Copyright 2022 University of California, Riverside
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
package edu.ucr.cs.bdlab.beast.geolite

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, Serializer}


/**
 * A Kryo serializer for RasterFeature.
 */
class RasterFeatureSerializer extends Serializer[RasterFeature]{
  override def write(kryo: Kryo, output: Output, rasterFeature: RasterFeature): Unit = {
    RasterSchemaHelper.write(rasterFeature.schema, kryo, output)
    kryo.writeClassAndObject(output, rasterFeature.getValues)
  }

  override def read(kryo: Kryo, input: Input, klass: Class[RasterFeature]): RasterFeature = {
    val schema = RasterSchemaHelper.readIn(kryo, input)
    val values = kryo.readClassAndObject(input).asInstanceOf[Array[Any]]
    new RasterFeature(values, schema)
  }
}