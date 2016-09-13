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
import org.apache.parquet.bytes.ByteBufferAllocator;
import org.apache.parquet.bytes.BytesInput;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.bytes.CapacityByteArrayOutputStream;
import org.apache.parquet.column.values.bitpacking.BytePacker;
import org.apache.parquet.column.values.bitpacking.BytePackerForLong;
import org.apache.parquet.column.values.bitpacking.Packer;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

import static org.apache.parquet.Log.DEBUG;

@SuppressWarnings("Duplicates")
public class VariableWidthLongRLEEncoder {
  private static final Logger LOG = LoggerFactory.getLogger(RunLengthBitPackingHybridEncoder.class);

  private final BytePackerForLong[] packers = new BytePackerForLong[64];

  private final CapacityByteArrayOutputStream baos;

  /**
   * Values that are bit packed 8 at at a time are packed into this
   * buffer, which is then written to baos
   */
  private final byte[] packBuffer;

  /**
   * The current width used in a bit packing run
   */
  private int currentWidth;

  /**
   * Previous value written, used to detect repeated values
   */
  private long previousValue;

  /**
   * We buffer 8 values at a time, and either bit pack them
   * or discard them after writing a rle-run
   */
  private final long[] bufferedValues;
  private int numBufferedValues;

  /**
   * How many times a value has been repeated
   */
  private int repeatCount;

  /**
   * How many groups of 8 values have been written
   * to the current bit-packed-run
   */
  private int bitPackedGroupCount;

  /**
   * A "pointer" to a single byte in baos,
   * which we use as our bit-packed-header. It's really
   * the logical index of the byte in baos.
   *
   * We are only using one byte for this header,
   * which limits us to writing 504 values per bit-packed-run.
   *
   * MSB must be 0 for varint encoding, LSB must be 1 to signify
   * that this is a bit-packed-header leaves 6 bits to write the
   * number of 8-groups -> (2^6 - 1) * 8 = 504
   */
  private long bitPackedRunHeaderPointer;

  private boolean toBytesCalled;

  public VariableWidthLongRLEEncoder(int initialCapacity, int pageSize, ByteBufferAllocator allocator) {
    if (DEBUG) {
      LOG.debug(String.format(
          "Encoding: RunLengthBitPackingHybridEncoder with initialCapacity %d",
          initialCapacity));
    }

    this.baos = new CapacityByteArrayOutputStream(initialCapacity, pageSize, allocator);
    this.packBuffer = new byte[64];
    this.bufferedValues = new long[8];
    this.currentWidth = 0;
    reset(false);
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

  private void reset(boolean resetBaos) {
    if (resetBaos) {
      this.baos.reset();
    }
    this.previousValue = 0;
    this.numBufferedValues = 0;
    this.repeatCount = 0;
    this.bitPackedGroupCount = 0;
    this.bitPackedRunHeaderPointer = -1;
    this.toBytesCalled = false;
  }

  public void writeLong(long value) throws IOException {
    if (value == previousValue) {
      // keep track of how many times we've seen this value
      // consecutively
      ++repeatCount;

      if (repeatCount >= 8) {
        // we've seen this at least 8 times, we're
        // certainly going to write an rle-run,
        // so just keep on counting repeats for now
        return;
      }
    } else {
      // This is a new value, check if it signals the end of
      // an rle-run
      if (repeatCount >= 8) {
        // it does! write an rle-run
        writeRleRun();
      }

      // this is a new value so we've only seen it once
      repeatCount = 1;
      // start tracking this value for repeats
      previousValue = value;
    }

    // We have not seen enough repeats to justify an rle-run yet,
    // so buffer this value in case we decide to write a bit-packed-run
    bufferedValues[numBufferedValues] = value;
    ++numBufferedValues;

    if (numBufferedValues == 8) {
      // we've encountered less than 8 repeated values, so
      // either start a new bit-packed-run or append to the
      // current bit-packed-run
      writeOrAppendBitPackedRun();
    }
  }

  private void writeOrAppendBitPackedRun() throws IOException {
    // find the width for this bit-packed run
    int requiredWidth = maxWidth(bufferedValues);

    boolean changeWidth = (
        (requiredWidth > currentWidth) || (currentWidth - requiredWidth >= 2));

    if (bitPackedGroupCount >= 63 || changeWidth) {
      // we've packed as many values as we can for this run,
      // end it and start a new one
      endPreviousBitPackedRun();
    }

    if (changeWidth) {
      this.currentWidth = requiredWidth;
    }

    if (bitPackedRunHeaderPointer == -1) {
      // this is a new bit-packed-run, allocate a byte for the header
      // and keep a "pointer" to it so that it can be mutated later
      baos.write(0); // write a sentinel value
      bitPackedRunHeaderPointer = baos.getCurrentIndex();
      baos.write((byte) currentWidth);
    }

    getPacker(currentWidth).pack8Values(bufferedValues, 0, packBuffer, 0);
    baos.write(packBuffer, 0, currentWidth);

    // empty the buffer, they've all been written
    numBufferedValues = 0;

    // clear the repeat count, as some repeated values
    // may have just been bit packed into this run
    repeatCount = 0;

    ++bitPackedGroupCount;
  }

  /**
   * If we are currently writing a bit-packed-run, update the
   * bit-packed-header and consider this run to be over
   *
   * does nothing if we're not currently writing a bit-packed run
   */
  private void endPreviousBitPackedRun() {
    if (bitPackedRunHeaderPointer == -1) {
      // we're not currently in a bit-packed-run
      return;
    }

    // create bit-packed-header, which needs to fit in 1 byte
    byte bitPackHeader = (byte) ((bitPackedGroupCount << 1) | 1);

    // update this byte
    baos.setByte(bitPackedRunHeaderPointer, bitPackHeader);

    // mark that this run is over
    bitPackedRunHeaderPointer = -1;

    // reset the number of groups
    bitPackedGroupCount = 0;
  }

  private void writeRleRun() throws IOException {
    // we may have been working on a bit-packed-run
    // so close that run if it exists before writing this
    // rle-run
    endPreviousBitPackedRun();

    // write the rle-header (lsb of 0 signifies a rle run)
    BytesUtils.writeUnsignedVarInt(repeatCount << 1, baos);
    // write the repeated-value
    BytesUtils.writeZigZagVarLong(previousValue, baos);

    // reset the repeat count
    repeatCount = 0;

    // throw away all the buffered values, they were just repeats and they've been written
    numBufferedValues = 0;
  }

  public BytesInput toBytes() throws IOException {
    Preconditions.checkArgument(!toBytesCalled,
        "You cannot call toBytes() more than once without calling reset()");

    // write anything that is buffered / queued up for an rle-run
    if (repeatCount >= 8) {
      writeRleRun();
    } else if(numBufferedValues > 0) {
      for (int i = numBufferedValues; i < 8; i++) {
        bufferedValues[i] = 0;
      }
      writeOrAppendBitPackedRun();
      endPreviousBitPackedRun();
    } else {
      endPreviousBitPackedRun();
    }

    toBytesCalled = true;
    return BytesInput.from(baos);
  }

  /**
   * Reset this encoder for re-use
   */
  public void reset() {
    reset(true);
  }

  public void close() {
    reset(false);
    baos.close();
  }

  public long getBufferedSize() {
    return baos.size();
  }

  public long getAllocatedSize() {
    return baos.getCapacity();
  }

  private int maxWidth(long[] longs) {
    long setBits = 0;
    for (int i = 0; i < longs.length; i += 1) {
      setBits |= longs[i];
    }
    // TODO: Long.numberOfTrailingZeros(setBits);
    return 64 - Long.numberOfLeadingZeros(setBits);
  }
}
