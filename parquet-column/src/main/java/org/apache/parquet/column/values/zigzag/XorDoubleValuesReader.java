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
import org.apache.parquet.bytes.ByteBufferInputStream;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * This ValuesReader does all the reading in {@link #initFromPage}
 * and stores the values in an in memory buffer, which is less than ideal.
 *
 * @author Alex Levenson
 */
public class XorDoubleValuesReader extends ValuesReader {
  private VariableWidthRLEDecoder exponentDecoder;
  private VariableWidthLongRLEDecoder mantissaDecoder;
  private long lastMantissaBits = 0L;
  private int nextOffset;

  @Override
  public void initFromPage(int valueCount, ByteBuffer page, int offset) throws IOException {
    ByteBufferInputStream in = new ByteBufferInputStream(page, offset, page.limit() - offset);

    int eLength = BytesUtils.readIntLittleEndian(in);
    // create a new input stream for the exponents block using the exponent len
    ByteBufferInputStream expIn = new ByteBufferInputStream(page, offset + 4, offset + 4 + eLength);
    // advance the original input stream to the start of the mantissa block
    long skipped = in.skip(eLength);
    Preconditions.checkState(skipped == eLength, "InputStream#skip failed");
    int mLength = BytesUtils.readIntLittleEndian(in);

    this.exponentDecoder = new VariableWidthRLEDecoder(expIn);
    this.mantissaDecoder = new VariableWidthLongRLEDecoder(in);
    this.lastMantissaBits = 0L;

    // 8 is for the two lengths which are stored as 4 bytes little endian
    this.nextOffset = offset + eLength + mLength + 8;
  }

  @Override
  public int getNextOffset() {
    return this.nextOffset;
  }

  @Override
  public double readDouble() {
    try {
      long mantissa = lastMantissaBits ^ mantissaDecoder.readLong();
      double d = ZigZagTransforms.Doubles.decode(
          exponentDecoder.readInt(), mantissa);
      this.lastMantissaBits = mantissa;
      return d;
    } catch (IOException e) {
      throw new ParquetDecodingException(e);
    }
  }

  @Override
  public void skip() {
    readDouble();
  }
}
