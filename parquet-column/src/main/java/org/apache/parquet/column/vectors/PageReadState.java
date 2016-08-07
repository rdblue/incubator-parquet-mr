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
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ValuesType;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DataPageV1;
import org.apache.parquet.column.page.DataPageV2;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.io.ParquetDecodingException;
import java.io.IOException;
import java.nio.ByteBuffer;

public class PageReadState {
  public final ColumnDescriptor column;
  private final PageReader pages;

  public int pageValueCount = 0;
  public ValuesReader rlReader = null;
  public ValuesReader dlReader = null;
  public ValuesReader vReader = null;

  private byte[] slop = new byte[0];
  public int slopLen = 0;

  public PageReadState(ColumnDescriptor column, PageReader pages) {
    this.column = column;
    this.pages = pages;
  }

  public void initFromPage(DataPage dataPage) {
    Preconditions.checkNotNull(dataPage, "Data page");
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
              "Failed to create readers for page %s in %s", page, column),
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
              "Failed to create readers for page %s in %s", page, column),
              e);
        }
        return null;
      }
    });
  }

  public final int valuesAvailable() {
    if (pageValueCount == 0) {
      DataPage page = pages.readPage();
      if (page == null) {
        return -1;
      }
      initFromPage(page);
    }
    return pageValueCount;
  }

  public void saveSlop(byte[] repetition, int offset, int length) {
    ensureSlopSpace(length);
    System.arraycopy(repetition, offset, slop, 0, length);
    this.slopLen = length;
  }

  public int loadSlop(byte[] repetition, int offset) {
    System.arraycopy(slop, 0, repetition, offset, slopLen);
    return slopLen;
  }

  private void ensureSlopSpace(int length) {
    if (slop.length < length) {
      slop = new byte[((length >> 10) + 1) << 10]; // next multiple of 1024
    }
  }
}
