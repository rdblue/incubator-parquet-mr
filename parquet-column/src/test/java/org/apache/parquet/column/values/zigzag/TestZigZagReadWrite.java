/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package org.apache.parquet.column.values.zigzag;

import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.ParquetProperties;
import org.junit.Assert;
import org.junit.Test;
import java.util.Random;

public class TestZigZagReadWrite {
  @Test
  public void testZigZagIntegerRandomData() throws Exception {
    Random actual = new Random(182389201011L);
    Random expected = new Random(182389201011L);

    ParquetProperties properties = ParquetProperties.builder().build();
    ZigZagIntValuesWriter writer = new ZigZagIntValuesWriter(
        properties.getInitialSlabSize(),
        properties.getPageSizeThreshold(),
        properties.getAllocator());

    int numValues = 10000000;
    for (int i = 0; i < numValues; i += 1) {
      writer.writeInteger(actual.nextInt());
    }

    BytesInput bytes = writer.getBytes();
    System.err.println("Encoded size: " + bytes.size());
    System.err.println("Compression ratio: " +
        ((double) bytes.size()) / 4 / numValues);
    ZigZagIntValuesReader reader = new ZigZagIntValuesReader();
    reader.initFromPage(10000000, bytes.toByteBuffer(), 0);
    for (int i = 0; i < numValues; i += 1) {
      Assert.assertEquals(String.valueOf(i),
          expected.nextInt(), reader.readInteger());
    }

    writer.close();
  }

  @Test
  public void testZigZagLongRandomData() throws Exception {
    Random actual = new Random(182389201011L);
    Random expected = new Random(182389201011L);

    ParquetProperties properties = ParquetProperties.builder().build();
    ZigZagLongValuesWriter writer = new ZigZagLongValuesWriter(
        properties.getInitialSlabSize(),
        properties.getPageSizeThreshold(),
        properties.getAllocator());

    int numValues = 10000000;
    for (int i = 0; i < numValues; i += 1) {
      writer.writeLong(actual.nextLong());
    }

    BytesInput bytes = writer.getBytes();
    System.err.println("Encoded size: " + bytes.size());
    System.err.println("Compression ratio: " +
        ((double) bytes.size()) / 8 / numValues);
    ZigZagLongValuesReader reader = new ZigZagLongValuesReader();
    reader.initFromPage(10000000, bytes.toByteBuffer(), 0);
    for (int i = 0; i < numValues; i += 1) {
      Assert.assertEquals(String.valueOf(i),
          expected.nextLong(), reader.readLong());
    }

    writer.close();
  }

  @Test
  public void testZigZagFloatRandomData() throws Exception {
    Random actual = new Random(182389201011L);
    Random expected = new Random(182389201011L);

    ParquetProperties properties = ParquetProperties.builder().build();
    ZigZagFloatValuesWriter writer = new ZigZagFloatValuesWriter(
        properties.getInitialSlabSize(),
        properties.getPageSizeThreshold(),
        properties.getAllocator());

    int numValues = 10000000;
    for (int i = 0; i < numValues; i += 1) {
      writer.writeFloat(actual.nextFloat());
    }

    BytesInput bytes = writer.getBytes();
    System.err.println("Encoded size: " + bytes.size());
    System.err.println("Compression ratio: " +
        ((double) bytes.size()) / 4 / numValues);
    ZigZagFloatValuesReader reader = new ZigZagFloatValuesReader();
    reader.initFromPage(10000000, bytes.toByteBuffer(), 0);
    for (int i = 0; i < numValues; i += 1) {
      try {
        // convert back to bits for comparison
        Assert.assertEquals(String.valueOf(i),
            Float.floatToRawIntBits(expected.nextFloat()),
            Float.floatToRawIntBits(reader.readFloat()));
      } catch (Exception e) {
        throw new RuntimeException("Failed on value " + i, e);
      }
    }

    writer.close();
  }

  @Test
  public void testZigZagDoubleRandomData() throws Exception {
    Random actual = new Random(182389201011L);
    Random expected = new Random(182389201011L);

    ParquetProperties properties = ParquetProperties.builder().build();
    ZigZagDoubleValuesWriter writer = new ZigZagDoubleValuesWriter(
        properties.getInitialSlabSize(),
        properties.getPageSizeThreshold(),
        properties.getAllocator());

    int numValues = 10000000;
    for (int i = 0; i < numValues; i += 1) {
      writer.writeDouble(actual.nextDouble());
    }

    BytesInput bytes = writer.getBytes();
    System.err.println("Encoded size: " + bytes.size());
    System.err.println("Compression ratio: " +
        ((double) bytes.size()) / 8 / numValues);
    ZigZagDoubleValuesReader reader = new ZigZagDoubleValuesReader();
    reader.initFromPage(10000000, bytes.toByteBuffer(), 0);
    for (int i = 0; i < numValues; i += 1) {
      try {
        // convert back to bits for comparison
        Assert.assertEquals(String.valueOf(i),
            Double.doubleToRawLongBits(expected.nextDouble()),
            Double.doubleToRawLongBits(reader.readDouble()));
      } catch (Exception e) {
        throw new RuntimeException("Failed on value " + i, e);
      }
    }

    writer.close();
  }

  @Test
  public void testDeltaIntRandomData() throws Exception {
    Random actual = new Random(182389201011L);
    Random expected = new Random(182389201011L);

    ParquetProperties properties = ParquetProperties.builder().build();
    DeltaZigZagIntValuesWriter writer = new DeltaZigZagIntValuesWriter(
        properties.getInitialSlabSize(),
        properties.getPageSizeThreshold(),
        properties.getAllocator());

    int numValues = 10000000;
    for (int i = 0; i < numValues; i += 1) {
      writer.writeInteger(actual.nextInt());
    }

    BytesInput bytes = writer.getBytes();
    System.err.println("Encoded size: " + bytes.size());
    System.err.println("Compression ratio: " +
        ((double) bytes.size()) / 4 / numValues);
    DeltaZigZagIntValuesReader reader = new DeltaZigZagIntValuesReader();
    reader.initFromPage(10000000, bytes.toByteBuffer(), 0);
    for (int i = 0; i < numValues; i += 1) {
      Assert.assertEquals(String.valueOf(i),
          expected.nextInt(), reader.readInteger());
    }

    writer.close();
  }

  @Test
  public void testDeltaLongRandomData() throws Exception {
    Random actual = new Random(182389201011L);
    Random expected = new Random(182389201011L);

    ParquetProperties properties = ParquetProperties.builder().build();
    DeltaZigZagLongValuesWriter writer = new DeltaZigZagLongValuesWriter(
        properties.getInitialSlabSize(),
        properties.getPageSizeThreshold(),
        properties.getAllocator());

    int numValues = 10000000;
    for (int i = 0; i < numValues; i += 1) {
      writer.writeLong(actual.nextLong());
    }

    BytesInput bytes = writer.getBytes();
    System.err.println("Encoded size: " + bytes.size());
    System.err.println("Compression ratio: " +
        ((double) bytes.size()) / 8 / numValues);
    DeltaZigZagLongValuesReader reader = new DeltaZigZagLongValuesReader();
    reader.initFromPage(10000000, bytes.toByteBuffer(), 0);
    for (int i = 0; i < numValues; i += 1) {
      Assert.assertEquals(String.valueOf(i),
          expected.nextLong(), reader.readLong());
    }

    writer.close();
  }

  @Test
  public void testXorFloatRandData() throws Exception {
    Random actual = new Random(182389201011L);
    Random expected = new Random(182389201011L);

    ParquetProperties properties = ParquetProperties.builder().build();
    XorFloatValuesWriter writer = new XorFloatValuesWriter(
        properties.getInitialSlabSize(),
        properties.getPageSizeThreshold(),
        properties.getAllocator());

    int numValues = 10000000;
    for (int i = 0; i < numValues; i += 1) {
      writer.writeFloat(actual.nextFloat());
    }

    BytesInput bytes = writer.getBytes();
    System.err.println("Encoded size: " + bytes.size());
    System.err.println("Compression ratio: " +
        ((double) bytes.size()) / 4 / numValues);
    XorFloatValuesReader reader = new XorFloatValuesReader();
    reader.initFromPage(10000000, bytes.toByteBuffer(), 0);
    for (int i = 0; i < numValues; i += 1) {
      try {
        // convert back to bits for comparison
        Assert.assertEquals(String.valueOf(i),
            Float.floatToRawIntBits(expected.nextFloat()),
            Float.floatToRawIntBits(reader.readFloat()));
      } catch (Exception e) {
        throw new RuntimeException("Failed on value " + i, e);
      }
    }

    writer.close();
  }

  @Test
  public void testXorDoubleRandData() throws Exception {
    Random actual = new Random(182389201011L);
    Random expected = new Random(182389201011L);

    ParquetProperties properties = ParquetProperties.builder().build();
    XorDoubleValuesWriter writer = new XorDoubleValuesWriter(
        properties.getInitialSlabSize(),
        properties.getPageSizeThreshold(),
        properties.getAllocator());

    int numValues = 10000000;
    for (int i = 0; i < numValues; i += 1) {
      writer.writeDouble(actual.nextDouble());
    }

    BytesInput bytes = writer.getBytes();
    System.err.println("Encoded size: " + bytes.size());
    System.err.println("Compression ratio: " +
        ((double) bytes.size()) / 8 / numValues);
    XorDoubleValuesReader reader = new XorDoubleValuesReader();
    reader.initFromPage(10000000, bytes.toByteBuffer(), 0);
    for (int i = 0; i < numValues; i += 1) {
      try {
        // convert back to bits for comparison
        Assert.assertEquals(String.valueOf(i),
            Double.doubleToRawLongBits(expected.nextDouble()),
            Double.doubleToRawLongBits(reader.readDouble()));
      } catch (Exception e) {
        throw new RuntimeException("Failed on value " + i, e);
      }
    }

    writer.close();
  }
}
