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

package org.apache.parquet.column;

import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.predicate.Operators;
import org.apache.parquet.filter2.predicate.UserDefinedPredicate;
import java.util.BitSet;

public class LongVector {
  public final long[] values;
  public final int[][] offsets;
  public final BitSet[] nullability;

  public LongVector() {
    // [ [7,4,1], [8,null,5] ],
    // [],
    // null,
    // [ [2,9,6,4], null, [] ]
    this.values = new long[] { 7, 4, 1, 8, 0, 5, 2, 9, 6, 4 };

    this.offsets = new int[][] {
        new int[] { 0, 1, 3, 4 },
        new int[] { 0, 3, 6, 10 }
    };

    BitSet firstLevel = new BitSet(4);
    firstLevel.set(0, 3);
    firstLevel.set(2, false);

    BitSet secondLevel = new BitSet(5);
    secondLevel.set(0, 4);
    secondLevel.set(3, false);

    BitSet thirdLevel = new BitSet(10);
    thirdLevel.set(0, 9);
    thirdLevel.set(4, false);

    this.nullability = new BitSet[] {
        firstLevel,
        secondLevel,
        thirdLevel
    };
  }

  public BitSet filter(Operators.ColumnFilterPredicate<Long> predicate) {
    if (offsets.length != 0) {
      return new BitSet(offsets[0].length);
    }
    return predicate.accept(new DenseVectorFilter());
  }

  public BitSet filter(BitSet selected, Operators.ColumnFilterPredicate<Long> predicate) {
    if (offsets.length != 0) {
      return new BitSet(offsets[0].length);
    }
    if (selected.cardinality() < (values.length >> 6)) {
      return predicate.accept(new SparseVectorFilter(selected));
    }
    return predicate.accept(new DenseVectorFilter());
  }

  class DenseVectorFilter implements FilterPredicate.Visitor<BitSet> {

    @Override
    public <T extends Comparable<T>> BitSet visit(Operators.Eq<T> eq) {
      BitSet result = new BitSet(values.length);
      if (isValueNull(eq)) {
        result.set(0, values.length - 1);
        result.andNot(nullability[0]);
      } else {
        long value = getValue(eq);
        for (int i = 0; i < values.length; i += 1) {
          result.set(i, values[i] == value);
        }
        result.and(nullability[0]);
      }
      return result;
    }

    @Override
    public <T extends Comparable<T>> BitSet visit(Operators.NotEq<T> notEq) {
      BitSet result = new BitSet(values.length);
      if (isValueNull(notEq)) {
        result.or(nullability[0]);
      }
      long value = getValue(notEq);
      for (int i = 0; i < values.length; i += 1) {
        result.set(i, values[i] != value);
      }
      result.and(nullability[0]);
      return result;
    }

    @Override
    public <T extends Comparable<T>> BitSet visit(Operators.Lt<T> lt) {
      BitSet result = new BitSet(values.length);
      long value = getValue(lt);
      for (int i = 0; i < values.length; i += 1) {
        result.set(i, values[i] < value);
      }
      result.and(nullability[0]);
      return result;
    }

    @Override
    public <T extends Comparable<T>> BitSet visit(Operators.LtEq<T> ltEq) {
      BitSet result = new BitSet(values.length);
      long value = getValue(ltEq);
      for (int i = 0; i < values.length; i += 1) {
        result.set(i, values[i] <= value);
      }
      result.and(nullability[0]);
      return result;
    }

    @Override
    public <T extends Comparable<T>> BitSet visit(Operators.Gt<T> gt) {
      BitSet result = new BitSet(values.length);
      long value = getValue(gt);
      for (int i = 0; i < values.length; i += 1) {
        result.set(i, values[i] > value);
      }
      result.and(nullability[0]);
      return result;
    }

    @Override
    public <T extends Comparable<T>> BitSet visit(Operators.GtEq<T> gtEq) {
      BitSet result = new BitSet(values.length);
      long value = getValue(gtEq);
      for (int i = 0; i < values.length; i += 1) {
        result.set(i, values[i] >= value);
      }
      result.and(nullability[0]);
      return result;
    }

    @Override
    public BitSet visit(Operators.And andPredicate) {
      BitSet result = andPredicate.getLeft().accept(this);
      result.and(andPredicate.getRight().accept(this));
      return result;
    }

    @Override
    public BitSet visit(Operators.Or orPredicate) {
      BitSet result = orPredicate.getLeft().accept(this);
      result.or(orPredicate.getRight().accept(this));
      return result;
    }

    @Override
    public BitSet visit(Operators.Not not) {
      BitSet result = new BitSet(values.length);
      result.set(0, values.length - 1);
      result.andNot(not.getPredicate().accept(this));
      return result;
    }

    @Override
    public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> BitSet visit(Operators.UserDefined<T, U> udp) {
      throw new UnsupportedOperationException("User-defined predicates aren't supported");
    }

    @Override
    public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> BitSet visit(Operators.LogicalNotUserDefined<T, U> udp) {
      throw new UnsupportedOperationException("User-defined predicates aren't supported");
    }
  }

  class SparseVectorFilter implements FilterPredicate.Visitor<BitSet> {
    private BitSet selected;

    public SparseVectorFilter(BitSet selected) {
      this.selected = selected;
    }

    @Override
    public <T extends Comparable<T>> BitSet visit(Operators.Eq<T> eq) {
      BitSet result = new BitSet(values.length);
      result.or(selected); // start with just the set that is selected
      if (isValueNull(eq)) {
        // remove the values that are not null
        result.andNot(nullability[0]);
      } else {
        result.and(nullability[0]);
        long value = getValue(eq);
        int next = 0;
        while ((next = result.nextSetBit(next)) > 0) {
          result.set(next, values[next] == value);
        }
      }
      return result;
    }

    @Override
    public <T extends Comparable<T>> BitSet visit(Operators.NotEq<T> notEq) {
      BitSet result = new BitSet(values.length);
      result.or(selected); // start with only hte set that is selected
      if (isValueNull(notEq)) {
        // remove the values that are null
        result.and(nullability[0]);
      } else {
        result.and(nullability[0]);
        long value = getValue(notEq);
        int next = 0;
        while ((next = result.nextSetBit(next)) > 0) {
          result.set(next, values[next] != value);
        }
      }
      return result;
    }

    @Override
    public <T extends Comparable<T>> BitSet visit(Operators.Lt<T> lt) {
      BitSet result = new BitSet(values.length);
      result.or(selected); // start with only hte set that is selected
      result.and(nullability[0]); // remove any values that are null
      long value = getValue(lt);
      int next = 0;
      while ((next = result.nextSetBit(next)) > 0) {
        result.set(next, values[next] < value);
      }
      return result;
    }

    @Override
    public <T extends Comparable<T>> BitSet visit(Operators.LtEq<T> ltEq) {
      BitSet result = new BitSet(values.length);
      result.or(selected); // start with only hte set that is selected
      result.and(nullability[0]); // remove any values that are null
      long value = getValue(ltEq);
      int next = 0;
      while ((next = result.nextSetBit(next)) > 0) {
        result.set(next, values[next] <= value);
      }
      return result;
    }

    @Override
    public <T extends Comparable<T>> BitSet visit(Operators.Gt<T> gt) {
      BitSet result = new BitSet(values.length);
      result.or(selected); // start with only hte set that is selected
      result.and(nullability[0]); // remove any values that are null
      long value = getValue(gt);
      int next = 0;
      while ((next = result.nextSetBit(next)) > 0) {
        result.set(next, values[next] > value);
      }
      return result;
    }

    @Override
    public <T extends Comparable<T>> BitSet visit(Operators.GtEq<T> gtEq) {
      BitSet result = new BitSet(values.length);
      result.or(selected); // start with only hte set that is selected
      result.and(nullability[0]); // remove any values that are null
      long value = getValue(gtEq);
      int next = 0;
      while ((next = result.nextSetBit(next)) > 0) {
        result.set(next, values[next] >= value);
      }
      return result;
    }

    @Override
    public BitSet visit(Operators.And andPredicate) {
      BitSet originalSelected = selected;
      this.selected = andPredicate.getLeft().accept(this);
      BitSet result = andPredicate.getRight().accept(this);
      this.selected = originalSelected;
      return result;
    }

    @Override
    public BitSet visit(Operators.Or orPredicate) {
      BitSet result = orPredicate.getLeft().accept(this);
      result.or(orPredicate.getRight().accept(this));
      return result;
    }

    @Override
    public BitSet visit(Operators.Not not) {
      BitSet result = new BitSet();
      result.or(selected);
      result.andNot(not.getPredicate().accept(this));
      return result;
    }

    @Override
    public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> BitSet visit(Operators.UserDefined<T, U> udp) {
      throw new UnsupportedOperationException("User-defined predicates aren't supported");
    }

    @Override
    public <T extends Comparable<T>, U extends UserDefinedPredicate<T>> BitSet visit(Operators.LogicalNotUserDefined<T, U> udp) {
      throw new UnsupportedOperationException("User-defined predicates aren't supported");
    }
  }

  private static boolean isValueNull(Operators.ColumnFilterPredicate<?> predicate) {
    return (predicate.getValue() == null);
  }

  private static long getValue(Operators.ColumnFilterPredicate<?> predicate) {
    Object value = predicate.getValue();
    if (value instanceof Long) {
      return (Long) value;
    } else {
      throw new IllegalArgumentException(
          "Can't process predicate: " + predicate);
    }
  }
}
