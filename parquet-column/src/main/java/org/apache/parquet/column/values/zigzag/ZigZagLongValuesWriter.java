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
public class ZigZagLongValuesWriter extends ValuesWriter {
  private final VariableWidthLongRLEEncoder encoder;

  public ZigZagLongValuesWriter(int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
    this.encoder = new VariableWidthLongRLEEncoder(initialCapacity, pageSize, allocator);
  }

  @Override
  public void writeLong(long v) {
    try {
      encoder.writeLong(ZigZagTransforms.Longs.encode(v));
    } catch (IOException e) {
      throw new ParquetEncodingException(e);
    }
  }

  @Override
  public long getBufferedSize() {
    return encoder.getBufferedSize();
  }

  @Override
  public long getAllocatedSize() {
    return encoder.getAllocatedSize();
  }

  @Override
  public BytesInput getBytes() {
    try {
      // prepend the length of the column
      BytesInput rle = encoder.toBytes();
      return BytesInput.concat(BytesInput.fromInt(Ints.checkedCast(rle.size())), rle);
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
    encoder.reset();
  }

  @Override
  public void close() {
    encoder.close();
  }

  @Override
  public String memUsageString(String prefix) {
    return String.format("%s RunLengthBitPackingHybrid %d bytes", prefix, getAllocatedSize());
  }
}
