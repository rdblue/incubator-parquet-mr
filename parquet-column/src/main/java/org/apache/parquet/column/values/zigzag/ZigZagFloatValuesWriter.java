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

import org.apache.parquet.Ints;
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.io.ParquetEncodingException;
import java.io.IOException;

/**
 * @author Alex Levenson
 */
public class ZigZagFloatValuesWriter extends ValuesWriter {
  private final VariableWidthRLEEncoder exponentEncoder;
  private final VariableWidthRLEEncoder mantissaEncoder;

  public ZigZagFloatValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
    this.exponentEncoder = new VariableWidthRLEEncoder(initialCapacity, pageSize, allocator);
    this.mantissaEncoder = new VariableWidthRLEEncoder(initialCapacity, pageSize, allocator);
  }

  @Override
  public void writeFloat(float v) {
    try {
      int bits = Float.floatToRawIntBits(v);
      exponentEncoder.writeInt(ZigZagTransforms.Floats.encodeExponent(bits));
      mantissaEncoder.writeInt(ZigZagTransforms.Floats.encodeMantissa(bits));
    } catch (IOException e) {
      throw new ParquetEncodingException(e);
    }
  }

  @Override
  public long getBufferedSize() {
    return
        exponentEncoder.getBufferedSize() + mantissaEncoder.getBufferedSize();
  }

  @Override
  public long getAllocatedSize() {
    return
        exponentEncoder.getAllocatedSize() + mantissaEncoder.getAllocatedSize();
  }

  @Override
  public BytesInput getBytes() {
    try {
      // prepend the length of the column
      BytesInput expBytes = exponentEncoder.toBytes();
      BytesInput manBytes = mantissaEncoder.toBytes();
      return BytesInput.concat(
          BytesInput.fromInt(Ints.checkedCast(expBytes.size())), expBytes,
          BytesInput.fromInt(Ints.checkedCast(manBytes.size())), manBytes);
    } catch (IOException e) {
      throw new ParquetEncodingException(e);
    }
  }

  @Override
  public Encoding getEncoding() {
    return Encoding.ZIGZAG_RLE;
  }

  @Override
  public void reset() {
    exponentEncoder.reset();
    mantissaEncoder.reset();
  }

  @Override
  public void close() {
    exponentEncoder.close();
    mantissaEncoder.close();
  }

  @Override
  public String memUsageString(String prefix) {
    return String.format("%s RunLengthBitPackingHybrid %d bytes", prefix, getAllocatedSize());
  }
}
