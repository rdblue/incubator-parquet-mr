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

package org.apache.parquet.column.vectors;

import org.apache.parquet.Preconditions;
import java.util.Arrays;
import java.util.BitSet;

public class IntVector {

  public int recordCapacity; // always <= length
  public int capacity; // index into arrays, so must be an int
  public int recordLength;
  public int length; // always <= capacity

  // limit is used to set the point up to which the vector has been used
  // copy(IntVector) then copies from this point to length into the new vector
  public int limit;

  public byte[] definition;
  public byte[] repetition;
  public int[] value;
  public BitSet nullability;

  private int growBy;

  public IntVector(int recordCapacity) {
    Preconditions.checkArgument(recordCapacity < maxValueCapacity(),
        "Record capacity cannot be larger than " + maxValueCapacity());
    this.recordCapacity = recordCapacity;
    this.capacity = recordCapacity;
    this.recordLength = 0;
    this.length = 0;
    this.growBy = recordCapacity;
    this.definition = new byte[capacity];
    this.repetition = new byte[capacity];
    this.value = new int[capacity];
    this.nullability = new BitSet(capacity);
  }

  public void ensureCapacity(int numValues) {
    int newCapacity;
    if (((long) length + numValues) < maxValueCapacity()) {
      newCapacity = length + numValues;
    } else {
      newCapacity = maxValueCapacity();
    }

    if (capacity > newCapacity) {
      return;
    }

    while (capacity < newCapacity) {
      this.capacity = capacity + growBy;
    }

    // TODO: it may be better to leave data in place and add new arrays
    this.definition = Arrays.copyOf(definition, capacity);
    this.repetition = Arrays.copyOf(repetition, capacity);
    this.value = Arrays.copyOf(value, capacity);
    BitSet nulls = nullability;
    this.nullability = new BitSet(capacity);
    nullability.or(nulls);
  }

  private void compact(BitSet keepSet) {

  }

  public static int maxValueCapacity() {
    return Integer.MAX_VALUE;
  }

}
