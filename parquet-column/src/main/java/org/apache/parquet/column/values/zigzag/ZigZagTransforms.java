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

public class ZigZagTransforms {

  public static class Ints {
    // Masks: for 0 (positive) all zeros, for 1 (negative) all 1s
    private static final int[] INT_MASKS = new int[] {
        0x00000000,
        0xFFFFFFFF
    };

    public static int encode(int n) {
      return (n << 1) ^ (n >> 31);
    }

    public static int decode(int n) {
      return INT_MASKS[0x00000001 & n] ^ (n >>> 1);
    }
  }

  public static class Longs {
    private static final long[] LONG_MASKS = new long[] {
        0x0000000000000000L,
        0xFFFFFFFFFFFFFFFFL
    };

    public static long encode(long n) {
      return (n << 1) ^ (n >> 63);
    }

    public static long decode(long n) {
      return LONG_MASKS[0x00000001 & ((int) n)] ^ (n >>> 1);
    }
  }

  public static class Floats {
    private static final int FLOAT_EXP_BIAS = 127;

    public static int encodeExponent(float f) {
      return encodeExponent(Float.floatToRawIntBits(f));
    }

    public static int encodeExponent(int fBits) {
      return Ints.encode(
          ((fBits & Integer.MAX_VALUE) >>> 23) - FLOAT_EXP_BIAS);
    }

    public static int encodeMantissa(float f) {
      return encodeMantissa(Float.floatToRawIntBits(f));
    }

    public static int encodeMantissa(int fBits) {
      return ((fBits & 0x7FFFFF) << 1) | ((fBits >> 31) & 1);
    }

    private static int decodeExponent(int exp) {
      return ((Ints.decode(exp) + FLOAT_EXP_BIAS) & 0xFF) << 23;
    }

    private static int decodeMantissa(int mantissa) {
      return (mantissa >>> 1) | (mantissa << 31);
    }

    public static float decode(int exp, int mantissa) {
      return Float.intBitsToFloat(
          decodeExponent(exp) | decodeMantissa(mantissa));
    }
  }

  public static class Doubles {
    private static final int DOUBLE_EXP_BIAS = 1023;

    public static int encodeExponent(double d) {
      return encodeExponent(Double.doubleToRawLongBits(d));
    }

    public static int encodeExponent(long fBits) {
      return Ints.encode(
          ((int) ((fBits & Long.MAX_VALUE) >>> 52)) - DOUBLE_EXP_BIAS);
    }

    public static long encodeMantissa(double d) {
      return encodeMantissa(Double.doubleToRawLongBits(d));
    }

    public static long encodeMantissa(long fBits) {
      return ((fBits & 0x000FFFFFFFFFFFFFL) << 1) | ((fBits >> 63) & 1);
    }

    // TODO: make private
    public static long decodeExponent(int exp) {
      return ((long) ((Ints.decode(exp) + DOUBLE_EXP_BIAS) & 0x7FF)) << 52;
    }

    // TODO: make private
    public static long decodeMantissa(long mantissa) {
      return (mantissa >>> 1) | (mantissa << 63);
    }

    public static double decode(int exp, long mantissa) {
      return Double.longBitsToDouble(
          decodeExponent(exp) | decodeMantissa(mantissa));
    }
  }
}
