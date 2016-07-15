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
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.MessageType;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;

public class VectorColumnReadStore implements ColumnReadStore {

  private final MessageType schema;
  private final GroupConverter converter;
  private final PageReadStore pageStore;
  private long rowGroupSize;

  public VectorColumnReadStore(PageReadStore pageStore, long rowGroupSize, MessageType schema, GroupConverter converter) {
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
    private DictionaryPage dict = null;

    public VectorColumnReader(PageReader pages, ColumnDescriptor column,
                              long columnLength) {
      this.pages = pages;
      this.column = column;
      this.columnLength = columnLength;
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

    // TODO: encapsulate this in a PageReadState
    private int pageValueCount = 0;
    private ValuesReader rlReader = null;
    private byte[] slop = new byte[0];
    private int slopLen = 0;
    private ValuesReader dlReader = null;
    private ValuesReader vReader = null;
    private boolean noMorePages = false;

    private void advancePage() {
      DataPage dataPage = pages.readPage();
      if (dataPage != null) {
        dataPage.accept(new DataPage.Visitor<Void>() {
          @Override
          public Void visit(DataPageV1 page) {
            try {
              pageValueCount = page.getValueCount();
              ByteBuffer bytes = page.getBytes().toByteBuffer();

              rlReader = page.getRlEncoding().getValuesReader(column, ValuesType.REPETITION_LEVEL);
              rlReader.initFromPage(pageValueCount, bytes, 0);

              dlReader = page.getDlEncoding().getValuesReader(column, ValuesType.DEFINITION_LEVEL);
              dlReader.initFromPage(pageValueCount, bytes, rlReader.getNextOffset());

              vReader = page.getValueEncoding().getValuesReader(column, ValuesType.VALUES);
              vReader.initFromPage(pageValueCount, bytes, dlReader.getNextOffset());
            } catch (IOException e) {
              throw new ParquetDecodingException(String.format(
                  "Failed to initialize readers for page %s in %s", page, column),
                  e);
            }
            return null;
          }

          @Override
          public Void visit(DataPageV2 page) {
            try {
              if (page.isCompressed()) {
                throw new IOException("Not implemented");
              }
            } catch (IOException e) {
              throw new ParquetDecodingException(String.format(
                  "Failed to initialize readers for page %s in %s", page, column),
                  e);
            }
            return null;
          }
        });
      } else {
        noMorePages = true;
      }
    }

    public int readVector(IntVector vector) {
      int pos = 0;
      int valuesRead;

      vector.recordLength = 0;
      int numRecords = vector.recordCapacity;

      if (column.getMaxRepetitionLevel() == 0) {
        // each value is a record
        while ((valuesRead = readValues(vector, pos, numRecords - pos)) > 0) {
          pos += valuesRead;
        }
        return pos;

      } else {
        int nextReadSize = Math.min(
            numRecords, IntVector.maxValueCapacity() - vector.length);
        while ((valuesRead = readValues(vector, pos, nextReadSize)) > 0) {
          pos += valuesRead;
          nextReadSize = Math.min(
              numRecords, IntVector.maxValueCapacity() - vector.length);
        }
      }

      return vector.recordLength;
    }

    public int readValues(IntVector vector, int offset, int numValues) {
      if (pageValueCount == 0) {
        advancePage();
        if (noMorePages) {
          return -1;
        }
      }

      int recordLimit = vector.recordCapacity - vector.recordLength;
      if (recordLimit <= 0) {
        return -1;
      }

      vector.ensureCapacity(numValues);

      // read to the end of the page or the number of values requested
      int valuesToRead = Math.min(numValues, pageValueCount);
      int limit = offset + valuesToRead;

      // keep track of record boundaries
      int recordCount = 0;

      if (column.getMaxRepetitionLevel() == 0) {
        recordCount = valuesToRead;
        for (int i = offset; i < limit; i += 1) {
          vector.repetition[i] = 0;
        }

      } else {
        if (slopLen >= valuesToRead) {
          return -1;
        }

        if (slopLen > 0) {
          System.arraycopy(slop, 0, vector.repetition, offset, slopLen);
        }

        // track the last position of each repetition level
        int[] last = new int[column.getMaxRepetitionLevel()];

        int i = offset + slopLen;
        last[0] = i;
        for (; i < limit && recordCount < recordLimit; i += 1) {
          byte r = (byte) rlReader.readInteger();
          vector.repetition[i] = r;
          recordCount += 1 - Integer.signum(r);
          last[r] = i; // TODO: is this better than a branch?
        }

        slopLen = i - last[0];
        if (slop.length < slopLen) {
          slop = new byte[((slopLen >> 10) + 1) << 10]; // next multiple of 1024
        }
        System.arraycopy(vector.repetition, last[0], slop, 0, slopLen);

        // update for slop and if the read stopped early
        valuesToRead = i - offset - slopLen;
        limit -= slopLen;
      }

      byte maxDefinitionLevel = (byte) column.getMaxDefinitionLevel();
      if (maxDefinitionLevel == 0) {
        for (int i = offset; i < limit; i += 1) {
          vector.definition[i] = 0;
        }
      } else {
        for (int i = offset; i < limit; i += 1) {
          byte d = (byte) dlReader.readInteger();
          vector.definition[i] = d;
          vector.nullability.set(i, d == maxDefinitionLevel);
        }
      }

      if ((maxDefinitionLevel > 0) &&
          (vector.nullability.cardinality() > (vector.capacity >> 5))) {
        int i = vector.nullability.nextSetBit(0);
        while (i > offset && i < limit) {
          vector.value[i] = vReader.readInteger();
          i = vector.nullability.nextSetBit(i);
        }
      } else {
        for (int i = offset; i < limit; i += 1) {
          if (vector.nullability.get(i)) {
            vector.value[i] = vReader.readInteger();
          }
        }
      }

      vector.recordLength += recordCount;
      this.pageValueCount -= valuesToRead;

      return valuesToRead;
    }

    public IntVector readVector(IntVector vector, BitSet keepSet) {
      // TODO
      return null;
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
      if (pos >= vector.length) {
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
