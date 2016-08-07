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

public class IntVector extends Vector {

  public final Vector.VectorSize size;

  public byte[] definition;
  public byte[] repetition;
  public int[] value;
  public BitSet nullability;

  public IntVector(int recordCapacity) {
    Preconditions.checkArgument(recordCapacity < maxValueCapacity(),
        "Record capacity cannot be larger than " + maxValueCapacity());
    this.size = new Vector.VectorSize(this, recordCapacity);
    this.definition = new byte[size.capacity];
    this.repetition = new byte[size.capacity];
    this.value = new int[size.capacity];
    this.nullability = new BitSet(size.capacity);
  }

  @Override
  public void reset() {
    size.length = 0;
    size.recordLength = 0;
    nullability.clear();
  }

  @Override
  public void resize() {
    // TODO: it may be better to leave data in place and add new arrays
    this.definition = Arrays.copyOf(definition, size.capacity);
    this.repetition = Arrays.copyOf(repetition, size.capacity);
    this.value = Arrays.copyOf(value, size.capacity);
    BitSet nulls = nullability;
    this.nullability = new BitSet(size.capacity);
    nullability.or(nulls);
  }

  private void compact(BitSet keepSet) {

  }


}
