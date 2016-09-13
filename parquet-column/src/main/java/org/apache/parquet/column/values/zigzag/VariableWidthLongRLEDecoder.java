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

import org.apache.parquet.Preconditions;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.BytePackerForLong;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridDecoder;
import org.apache.parquet.io.ParquetDecodingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

@SuppressWarnings("Duplicates")
public class VariableWidthLongRLEDecoder {
  private static final Logger LOG = LoggerFactory.getLogger(RunLengthBitPackingHybridDecoder.class);

  private enum MODE { RLE, PACKED }

  private final BytePackerForLong[] packers = new BytePackerForLong[64];

  private final InputStream in;

  private MODE mode;
  private int currentCount;
  private long currentValue;
  private long[] currentBuffer;

  public VariableWidthLongRLEDecoder(InputStream in) {
    this.in = in;
  }

  public long readLong() throws IOException {
    if (currentCount == 0) {
      readNext();
    }
    -- currentCount;
    long result;
    switch (mode) {
      case RLE:
        result = currentValue;
        break;
      case PACKED:
        result = currentBuffer[currentBuffer.length - 1 - currentCount];
        break;
      default:
        throw new ParquetDecodingException("not a valid mode " + mode);
    }
    return result;
  }

  private void readNext() throws IOException {
    Preconditions.checkArgument(in.available() > 0, "Reading past RLE/BitPacking stream.");
    final int header = BytesUtils.readUnsignedVarInt(in);
    mode = (header & 1) == 0 ? MODE.RLE : MODE.PACKED;
    switch (mode) {
      case RLE:
        currentCount = header >>> 1;
        LOG.debug("reading " + currentCount + " values RLE");
        currentValue = BytesUtils.readZigZagVarLong(in);
        break;
      case PACKED:
        int width = BytesUtils.readUnsignedVarInt(in);
        int numGroups = header >>> 1;
        currentCount = numGroups * 8;
        LOG.debug("reading " + currentCount + " values BIT PACKED");
        currentBuffer = new long[currentCount]; // TODO: reuse a buffer
        byte[] bytes = new byte[numGroups * width];
        // At the end of the file RLE data though, there might not be that many bytes left.
        int bytesToRead = (int)Math.ceil(currentCount * width / 8.0);
        bytesToRead = Math.min(bytesToRead, in.available());
        new DataInputStream(in).readFully(bytes, 0, bytesToRead);
        for (int valueIndex = 0, byteIndex = 0; valueIndex < currentCount; valueIndex += 8, byteIndex += width) {
          getPacker(width).unpack8Values(bytes, byteIndex, currentBuffer, valueIndex);
        }
        break;
      default:
        throw new ParquetDecodingException("not a valid mode " + mode);
    }
  }

  private BytePackerForLong getPacker(int bitWidth) {
    Preconditions.checkArgument(bitWidth <= 64,
        "Cannot create packer for width " + bitWidth);
    if (bitWidth == 0) {
      // TODO!
      bitWidth = 1;
    }

    BytePackerForLong packer = packers[bitWidth - 1];
    if (packer == null) {
      packer = Packer.LITTLE_ENDIAN.newBytePackerForLong(bitWidth);
      packers[bitWidth - 1] = packer;
    }
    return packer;
  }
}
