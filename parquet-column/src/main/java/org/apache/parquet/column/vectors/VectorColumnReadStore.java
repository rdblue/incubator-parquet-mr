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

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import java.util.BitSet;

public class VectorColumnReadStore implements ColumnReadStore {

  private final MessageType schema;
  private final GroupConverter converter;
  private final PageReadStore pageStore;
  private long rowGroupSize;

  public VectorColumnReadStore(PageReadStore pageStore, long rowGroupSize,
                               MessageType schema, GroupConverter converter) {
    this.pageStore = pageStore;
    this.rowGroupSize = rowGroupSize;
    this.schema = schema;
    this.converter = converter;
  }

  public VectorColumnReader getVectorColumnReader(ColumnDescriptor column) {
    return new VectorColumnReader(
        pageStore.getPageReader(column), column, rowGroupSize);
  }

  @Override
  public ColumnReader getColumnReader(ColumnDescriptor column) {
    return new InternalVectorColumnReader(
        new VectorColumnReader(pageStore.getPageReader(column), column, rowGroupSize),
        ColumnReadStoreImpl.getPrimitiveConverter(schema, converter, column));
  }

  public static class VectorColumnReader {
    private final PageReader pages;
    private final ColumnDescriptor column;
    private final long columnLength;
    private final PageReadState state;
    private final DictionaryPage dict;

    public VectorColumnReader(PageReader pages, ColumnDescriptor column,
                              long columnLength) {
      this.pages = pages;
      this.column = column;
      this.columnLength = columnLength;
      this.state = new PageReadState(column, pages);
      this.dict = pages.readDictionaryPage();
    }

    public long getColumnLength() {
      return columnLength;
    }

    public ColumnDescriptor getColumnDescriptor() {
      return column;
    }

    public DictionaryPage getDictionaryPage() {
      return dict;
    }

    public int readVector(IntVector vector) {
      int pos = 0;
      int valuesRead;

      vector.reset();
      int numRecords = vector.size.recordCapacity;

      if (column.getMaxRepetitionLevel() == 0) {
        // each value is a record
        if (column.getMaxDefinitionLevel() == 0) {
          while ((valuesRead = readRl0(state, vector.size, vector.repetition, pos)) > 0) {
            readDl0(vector.definition, vector.nullability, pos, valuesRead);
            readNonNullValues(state, vector, pos, valuesRead);
            pos += valuesRead;
          }
        } else {
          while ((valuesRead = readRl0(state, vector.size, vector.repetition, pos)) > 0) {
            readDl(state, vector.definition, vector.nullability, pos, valuesRead);
            readNullableValues(state, vector, pos, valuesRead);
            pos += valuesRead;
          }
        }

      } else {
        if (column.getMaxDefinitionLevel() == 0) {
          while ((valuesRead = readRl(state, vector.size, vector.repetition, pos, numRecords)) > 0) {
            readDl0(vector.definition, vector.nullability, pos, valuesRead);
            readNonNullValues(state, vector, pos, valuesRead);
            pos += valuesRead;
          }
        } else {
          while ((valuesRead = readRl(state, vector.size, vector.repetition, pos, numRecords)) > 0) {
            readDl(state, vector.definition, vector.nullability, pos, valuesRead);
            readNullableValues(state, vector, pos, valuesRead);
            pos += valuesRead;
          }
        }
      }

      return vector.size.recordLength;
    }

    private static int readRl0(
        PageReadState state, Vector.VectorSize size, byte[] repetition,
        int offset) {
      int numValues = size.availableRecordCapacity();

      int pageValues = state.valuesAvailable();
      if (pageValues < 0) {
        return -1;
      }

      // read to the end of the page, until the vector is full, or the number
      // of values requested
      int valuesToRead = Math.min(numValues, pageValues);

      int limit = offset + valuesToRead;
      for (int i = offset; i < limit; i += 1) {
        repetition[i] = 0;
      }

      size.length += valuesToRead;
      size.recordLength += valuesToRead;
      state.pageValueCount -= valuesToRead;

      return valuesToRead;
    }

    private static int readRl(
        PageReadState state, Vector.VectorSize size, byte[] repetition,
        int offset, int numValues) {
      int recordsToRead = size.availableRecordCapacity();
      if (recordsToRead <= 0) {
        return -1;
      }

      int pageValues = state.valuesAvailable();
      if (pageValues < 0) {
        return -1;
      }

      size.ensureCapacity(numValues);
      int availableCapacity = size.availableCapacity();
      if (availableCapacity <= 0) {
        return -1;
      }

      // read to the end of the page, until the vector is full, or the number
      // of values requested
      int valuesToRead = Math.min(
          Math.min(numValues, state.pageValueCount),
          availableCapacity);

      int limit = offset + valuesToRead;

      if (state.slopLen >= valuesToRead) {
        return -1;
      }

      int readFromSlop = state.loadSlop(repetition, offset);

      // keep track of record boundaries
      int recordCount = 0;
      int[] rlast = new int[state.column.getMaxRepetitionLevel()];

      int i = offset + readFromSlop;
      rlast[0] = i;
      for (; i < limit && recordCount < recordsToRead; i += 1) {
        byte r = (byte) state.rlReader.readInteger();
        repetition[i] = r;
        recordCount += 1 - Integer.signum(r);
        rlast[r] = i; // TODO: is this better than a branch?
      }

      state.saveSlop(repetition, rlast[0], i - rlast[0]);

      // update the vector length now that the number of triples is known
      int valuesRead = rlast[0] - offset;
      size.recordLength += recordCount;
      size.length += valuesRead;

      // this should be the number of values available to read, regardless of
      // whether this read too many repetition levels. (slop counts for total)
      state.pageValueCount -= valuesRead;

      return valuesRead;
    }

    private static void readDl0(
        byte[] definition, BitSet nullability, int offset, int valuesToRead) {
      int limit = offset + valuesToRead;

      // definition level is always 0
      for (int i = offset; i < limit; i += 1) {
        definition[i] = 0;
      }

      // values are never null
      nullability.set(offset, limit, true);
    }

    private static void readDl(
        PageReadState state, byte[] definition, BitSet nullability, int offset,
        int valuesToRead) {
      int limit = offset + valuesToRead;

      byte maxDl = (byte) state.column.getMaxDefinitionLevel();
      for (int i = offset; i < limit; i += 1) {
        byte d = (byte) state.dlReader.readInteger();
        definition[i] = d;
        nullability.set(i, d == maxDl);
      }
    }

    private static void readNonNullValues(
        PageReadState state, IntVector vector, int offset, int valuesToRead) {
      int limit = offset + valuesToRead;

      for (int i = offset; i < limit; i += 1) {
        vector.value[i] = state.vReader.readInteger();
      }
    }

    private static void readNullableValues(
        PageReadState state, IntVector vector, int offset, int valuesToRead) {
      int limit = offset + valuesToRead;

      if (vector.nullability.cardinality() < (vector.size.length >> 5)) {
        int i = vector.nullability.nextSetBit(0);
        while (i > offset && i < limit) {
          vector.value[i] = state.vReader.readInteger();
          i = vector.nullability.nextSetBit(i);
        }
      } else {
        for (int i = offset; i < limit; i += 1) {
          if (vector.nullability.get(i)) {
            vector.value[i] = state.vReader.readInteger();
          }
        }
      }
    }

    public int readVector(IntVector vector, BitSet keepSet) {
      // TODO
      return 0;
    }
  }

  public static class InternalVectorColumnReader implements ColumnReader {
    private final VectorColumnReader vectorReader;
    private final PrimitiveConverter converter;
    private final IntVector vector;
    private int pos;

    public InternalVectorColumnReader(VectorColumnReader vectorReader,
                                      PrimitiveConverter converter) {
      this.vectorReader = vectorReader;
      this.converter = converter;
      this.vector = new IntVector(8192);
      this.pos = 0;
      read();
    }

    private void read() {
      vectorReader.readVector(vector);
    }

    @Override
    public long getTotalValueCount() {
      return vectorReader.getColumnLength();
    }

    @Override
    public void consume() {
      this.pos += 1;
      if (pos >= vector.size.length) {
        read();
      }
      pos = 0;
    }

    @Override
    public int getCurrentRepetitionLevel() {
      return vector.repetition[pos];
    }

    @Override
    public int getCurrentDefinitionLevel() {
      return vector.definition[pos];
    }

    @Override
    public void writeCurrentValueToConverter() {
      converter.addInt(vector.value[pos]);
    }

    @Override
    public void skip() {
      this.pos += 1;
    }

    @Override
    public int getCurrentValueDictionaryID() {
      return vector.value[pos];
    }

    @Override
    public int getInteger() {
      return vector.value[pos];
    }

    @Override
    public boolean getBoolean() {
      throw new UnsupportedOperationException("Integer column");
    }

    @Override
    public long getLong() {
      throw new UnsupportedOperationException("Integer column");
    }

    @Override
    public Binary getBinary() {
      throw new UnsupportedOperationException("Integer column");
    }

    @Override
    public float getFloat() {
      throw new UnsupportedOperationException("Integer column");
    }

    @Override
    public double getDouble() {
      throw new UnsupportedOperationException("Integer column");
    }

    @Override
    public ColumnDescriptor getDescriptor() {
      return vectorReader.getColumnDescriptor();
    }
  }
}
