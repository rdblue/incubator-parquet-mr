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

public class TestVariableWidthRLE {
  @Test
  public void testRandomData() throws Exception {
    Random actual = new Random(81892318932L);
    Random expected = new Random(81892318932L);

    ParquetProperties properties = ParquetProperties.builder().build();
    VariableWidthRLEEncoder encoder = new VariableWidthRLEEncoder(
        properties.getInitialSlabSize(),
        properties.getPageSizeThreshold(),
        properties.getAllocator());

    int numValues = 10000000;
    for (int i = 0; i < numValues; i += 1) {
      encoder.writeInt(actual.nextInt(1000));
    }

    BytesInput bytes = encoder.toBytes();
    System.err.println("Encoded size: " + bytes.size());
    System.err.println("Compression ratio: " +
        ((double) bytes.size()) / 4 / numValues);
    VariableWidthRLEDecoder decoder = new VariableWidthRLEDecoder(
        bytes.toInputStream());
    for (int i = 0; i < numValues; i += 1) {
      Assert.assertEquals(String.valueOf(i),
          expected.nextInt(1000), decoder.readInt());
    }

    encoder.close();
  }

  @Test
  public void testRandomDataWithRLE() throws Exception {
    Random actual = new Random(81892318932L);
    Random expected = new Random(81892318932L);

    ParquetProperties properties = ParquetProperties.builder().build();
    VariableWidthRLEEncoder encoder = new VariableWidthRLEEncoder(
        properties.getInitialSlabSize(),
        properties.getPageSizeThreshold(),
        properties.getAllocator());

    int numValues = 10000000;
    for (int i = 0; i < numValues; i += 1) {
      // a small set of values ensures that there will be random RLE runs
      encoder.writeInt(actual.nextInt(5));
    }

    BytesInput bytes = encoder.toBytes();
    System.err.println("Encoded size: " + bytes.size());
    System.err.println("Compression ratio: " +
        ((double) bytes.size()) / 4 / numValues);
    VariableWidthRLEDecoder decoder = new VariableWidthRLEDecoder(
        bytes.toInputStream());
    for (int i = 0; i < numValues; i += 1) {
      Assert.assertEquals(String.valueOf(i),
          expected.nextInt(5), decoder.readInt());
    }

    encoder.close();
  }

  @Test
  public void testZigZagEncoding() throws Exception {
    Random normal = new Random(81892318932L);
    Random zigzag = new Random(81892318932L);
    Random expected = new Random(81892318932L);

    ParquetProperties properties = ParquetProperties.builder().build();
    VariableWidthRLEEncoder normalEncoder = new VariableWidthRLEEncoder(
        properties.getInitialSlabSize(),
        properties.getPageSizeThreshold(),
        properties.getAllocator());

    int numValues = 10000000;
    for (int i = 0; i < numValues; i += 1) {
      // a small set of values ensures that there will be random RLE runs
      normalEncoder.writeInt(normal.nextInt(1000) - 500);
    }

    long normalSize = normalEncoder.toBytes().size();

    VariableWidthRLEEncoder zigzagEncoder = new VariableWidthRLEEncoder(
        properties.getInitialSlabSize(),
        properties.getPageSizeThreshold(),
        properties.getAllocator());

    for (int i = 0; i < numValues; i += 1) {
      zigzagEncoder.writeInt(zigzag(zigzag.nextInt(1000) - 500));
    }

    BytesInput bytes = zigzagEncoder.toBytes();
    System.err.println("Normal size: " + normalSize);
    System.err.println("Compression ratio: " +
        ((double) normalSize) / 4 / numValues);
    System.err.println("Zigzag size: " + bytes.size());
    System.err.println("Compression ratio: " +
        ((double) bytes.size()) / 4 / numValues);
    VariableWidthRLEDecoder decoder = new VariableWidthRLEDecoder(
        bytes.toInputStream());
    for (int i = 0; i < numValues; i += 1) {
      Assert.assertEquals(String.valueOf(i),
          zigzag(expected.nextInt(1000) - 500), decoder.readInt());
    }

    zigzagEncoder.close();
    normalEncoder.close();
  }

  private int zigzag(int n) {
    return (n << 1) ^ (n >> 31);
  }
}
