/**
 * Copyright 2012 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package parquet.avro;

import it.unimi.dsi.fastutil.booleans.BooleanArrayList;
import it.unimi.dsi.fastutil.bytes.ByteArrayList;
import it.unimi.dsi.fastutil.chars.CharArrayList;
import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.shorts.ShortArrayList;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import parquet.Preconditions;
import parquet.io.InvalidRecordException;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.Type;

class AvroIndexedRecordConverter<T> extends GroupConverter {

  private final ParentValueContainer parent;
  protected T currentRecord;
  private final Converter[] converters;

  private final Schema avroSchema;

  private final GenericData model;
  private final Map<Schema.Field, Object> recordDefaults = new HashMap<Schema.Field, Object>();

  public AvroIndexedRecordConverter(MessageType parquetSchema,
                                    Schema avroSchema, GenericData model) {
    this(null, parquetSchema, avroSchema, model);
  }

  public AvroIndexedRecordConverter(ParentValueContainer parent,
                                    GroupType parquetSchema, Schema avroSchema,
                                    GenericData model) {
    this.parent = parent;
    this.avroSchema = avroSchema;
    int schemaSize = parquetSchema.getFieldCount();
    this.converters = new Converter[schemaSize];
    this.model = (model == null) ? ReflectData.get() : model;

    Map<String, Integer> avroFieldIndexes = new HashMap<String, Integer>();
    int avroFieldIndex = 0;
    for (Schema.Field field: avroSchema.getFields()) {
        avroFieldIndexes.put(field.name(), avroFieldIndex++);
    }
    int parquetFieldIndex = 0;
    for (Type parquetField: parquetSchema.getFields()) {
      final Schema.Field avroField = getAvroField(parquetField.getName());
      Schema nonNullSchema = AvroSchemaConverter.getNonNull(avroField.schema());
      final int finalAvroIndex = avroFieldIndexes.remove(avroField.name());
      converters[parquetFieldIndex++] = newConverter(
          this.model, nonNullSchema, parquetField, new ParentValueContainer() {
        @Override
        void add(Object value) {
          AvroIndexedRecordConverter.this.set(avroField.name(), finalAvroIndex, value);
        }
      });
    }
    // store defaults for any new Avro fields from avroSchema that are not in the writer schema (parquetSchema)
    for (String fieldName : avroFieldIndexes.keySet()) {
      Schema.Field field = avroSchema.getField(fieldName);
      if (field.schema().getType() == Schema.Type.NULL) {
        continue; // skip null since Parquet does not write nulls
      }
      // use this.model because model may be null
      recordDefaults.put(field, this.model.getDefaultValue(field));
    }
  }

  private Schema.Field getAvroField(String parquetFieldName) {
    Schema.Field avroField = avroSchema.getField(parquetFieldName);
    for (Schema.Field f : avroSchema.getFields()) {
      if (f.aliases().contains(parquetFieldName)) {
        return f;
      }
    }
    if (avroField == null) {
      throw new InvalidRecordException(String.format("Parquet/Avro schema mismatch. Avro field '%s' not found.",
          parquetFieldName));
    }
    return avroField;
  }

  private static Converter newConverter(GenericData model, Schema schema, Type type,
      ParentValueContainer parent) {
    if (schema.getType().equals(Schema.Type.BOOLEAN)) {
      return new FieldBooleanConverter(parent);
    } else if (schema.getType().equals(Schema.Type.INT)) {
      Class<?> intClass = getSpecificClass(model, schema);
      if (byte.class.equals(intClass)) {
        return new FieldByteConverter(parent);
      } else if (short.class.equals(intClass)) {
        return new FieldShortConverter(parent);
      } else if (char.class.equals(intClass)) {
        return new FieldCharConverter(parent);
      }
      return new FieldIntegerConverter(parent);
    } else if (schema.getType().equals(Schema.Type.LONG)) {
      return new FieldLongConverter(parent);
    } else if (schema.getType().equals(Schema.Type.FLOAT)) {
      return new FieldFloatConverter(parent);
    } else if (schema.getType().equals(Schema.Type.DOUBLE)) {
      return new FieldDoubleConverter(parent);
    } else if (schema.getType().equals(Schema.Type.BYTES)) {
      if (isReflectedArray(model, schema)) {
        return new FieldBytesConverter(parent);
      } else {
        return new FieldByteBufferConverter(parent);
      }
    } else if (schema.getType().equals(Schema.Type.STRING)) {
      return new FieldStringConverter(parent);
    } else if (schema.getType().equals(Schema.Type.RECORD)) {
      return new AvroIndexedRecordConverter(
          parent, type.asGroupType(), schema, model);
    } else if (schema.getType().equals(Schema.Type.ENUM)) {
      return new FieldEnumConverter(parent, model, schema);
    } else if (schema.getType().equals(Schema.Type.ARRAY)) {
      return new AvroCollectionConverter(parent, type, model, schema);
    } else if (schema.getType().equals(Schema.Type.MAP)) {
      return new MapConverter(parent, type, model, schema);
    } else if (schema.getType().equals(Schema.Type.UNION)) {
      return new AvroUnionConverter(parent, type, model, schema);
    } else if (schema.getType().equals(Schema.Type.FIXED)) {
      return new FieldFixedConverter(parent, model, schema);
    }
    throw new UnsupportedOperationException(String.format("Cannot convert Avro type: %s" +
        " (Parquet type: %s) ", schema, type));
  }

  private static boolean isReflectedArray(GenericData model, Schema schema) {
    return (model instanceof SpecificData &&
        ((SpecificData) model).getClass(schema).isArray());
  }

  @SuppressWarnings("unchecked")
  private static <T> Class<T> getSpecificClass(GenericData model, Schema schema) {
    if (model instanceof SpecificData) {
      return ((SpecificData) model).getClass(schema);
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private static <T> Class<T> getElementClass(Class<?> someClass) {
    if (someClass != null && someClass.isArray()) {
      return (Class<T>) someClass.getComponentType();
    }
    return null;
  }

  private void set(String name, int index, Object value) {
    this.model.setField(this.currentRecord, name, index, value);
  }

  @Override
  public Converter getConverter(int fieldIndex) {
    return converters[fieldIndex];
  }

  @Override
  public void start() {
    this.currentRecord = (T) model.newRecord(null, avroSchema);
  }

  @Override
  public void end() {
    fillInDefaults();
    if (parent != null) {
      parent.add(currentRecord);
    }
  }

  private void fillInDefaults() {
    for (Map.Entry<Schema.Field, Object> entry : recordDefaults.entrySet()) {
      Schema.Field f = entry.getKey();
      // replace following with model.deepCopy once AVRO-1455 is being used
      Object defaultValue = deepCopy(f.schema(), entry.getValue());
      this.model.setField(this.currentRecord, f.name(), f.pos(), defaultValue);
    }
  }

  private Object deepCopy(Schema schema, Object value) {
    switch (schema.getType()) {
      case BOOLEAN:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return value;
      default:
        return model.deepCopy(schema, value);
    }
  }

  T getCurrentRecord() {
    return currentRecord;
  }

  static abstract class ParentValueContainer {

    /**
     * Adds the value to the parent.
     */
    abstract void add(Object value);

  }

  static final class FieldBooleanConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldBooleanConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBoolean(boolean value) {
      parent.add(value);
    }

  }

  static final class FieldByteConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldByteConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addInt(int value) {
      parent.add((byte) value);
    }

  }

  static final class FieldShortConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldShortConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addInt(int value) {
      parent.add((short) value);
    }

  }

  static final class FieldCharConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldCharConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addInt(int value) {
      parent.add((char) value);
    }

  }

  static final class FieldIntegerConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldIntegerConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addInt(int value) {
      parent.add(value);
    }

  }

  static final class FieldLongConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldLongConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addInt(int value) {
      parent.add(Long.valueOf(value));
    }

    @Override
    final public void addLong(long value) {
      parent.add(value);
    }

  }

  static final class FieldFloatConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldFloatConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addInt(int value) {
      parent.add(Float.valueOf(value));
    }

    @Override
    final public void addLong(long value) {
      parent.add(Float.valueOf(value));
    }

    @Override
    final public void addFloat(float value) {
      parent.add(value);
    }

  }

  static final class FieldDoubleConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldDoubleConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addInt(int value) {
      parent.add(Double.valueOf(value));
    }

    @Override
    final public void addLong(long value) {
      parent.add(Double.valueOf(value));
    }

    @Override
    final public void addFloat(float value) {
      parent.add(Double.valueOf(value));
    }

    @Override
    final public void addDouble(double value) {
      parent.add(value);
    }

  }

  static final class FieldBytesConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldBytesConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(value.getBytes());
    }

  }

  static final class FieldByteBufferConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldByteBufferConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(ByteBuffer.wrap(value.getBytes()));
    }

  }

  static final class FieldStringConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;

    public FieldStringConverter(ParentValueContainer parent) {
      this.parent = parent;
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(value.toStringUsingUTF8());
    }

  }

  static final class FieldEnumConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;
    private final GenericData model;
    private final Schema enumSchema;

    public FieldEnumConverter(ParentValueContainer parent, GenericData model, Schema enumSchema) {
      this.parent = parent;
      this.model = model;
      this.enumSchema = enumSchema;
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(model.createEnum(value.toStringUsingUTF8(), enumSchema));
    }
  }

  static final class FieldFixedConverter extends PrimitiveConverter {

    private final ParentValueContainer parent;
    private final GenericData model;
    private final Schema avroSchema;

    public FieldFixedConverter(ParentValueContainer parent, GenericData model, Schema avroSchema) {
      this.parent = parent;
      this.model = model;
      this.avroSchema = avroSchema;
    }

    @Override
    final public void addBinary(Binary value) {
      parent.add(model.createFixed(null /* obj to reuse */ , value.getBytes(),
          avroSchema));
    }
  }

  static class AvroCollectionConverter<T> extends GroupConverter {

    protected final ParentValueContainer parent;
    protected final GenericData model;
    protected final Schema avroSchema;
    protected final Class<?> repeatedClass;
    protected final Class<T> elementClass;
    private final Converter converter;
    protected Collection<T> collection;

    public AvroCollectionConverter(ParentValueContainer parent, Type parquetSchema,
        GenericData model, Schema avroSchema) {
      this.parent = parent;
      this.model = model;
      this.avroSchema = avroSchema;
      this.repeatedClass = getSpecificClass(model, avroSchema);
      this.elementClass = getElementClass(repeatedClass);

      Type elementType = parquetSchema.asGroupType().getType(0);
      Schema elementSchema = avroSchema.getElementType();
      converter = newConverter(model, elementSchema, elementType, new ParentValueContainer() {
        @Override
        @SuppressWarnings("unchecked")
        void add(Object value) {
          collection.add((T) value);
        }
      });
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converter;
    }

    @Override
    public void start() {
      collection = newCollection();
    }

    @Override
    public void end() {
      if (repeatedClass != null && repeatedClass.isArray()) {
        // convert the accumulated List into an array
        if (elementClass.equals(boolean.class)) {
          parent.add(((BooleanArrayList) collection)
              .toBooleanArray(new boolean[collection.size()]));
        } else if (elementClass.equals(char.class)) {
          parent.add(((CharArrayList) collection)
              .toCharArray(new char[collection.size()]));
        } else if (elementClass.equals(byte.class)) {
          parent.add(((ByteArrayList) collection)
              .toByteArray(new byte[collection.size()]));
        } else if (elementClass.equals(short.class)) {
          parent.add(((ShortArrayList) collection)
              .toShortArray(new short[collection.size()]));
        } else if (elementClass.equals(int.class)) {
          parent.add(((IntArrayList) collection)
              .toIntArray(new int[collection.size()]));
        } else if (elementClass.equals(long.class)) {
          parent.add(((LongArrayList) collection)
              .toLongArray(new long[collection.size()]));
        } else if (elementClass.equals(float.class)) {
          parent.add(((FloatArrayList) collection)
              .toFloatArray(new float[collection.size()]));
        } else if (elementClass.equals(double.class)) {
          parent.add(((DoubleArrayList) collection)
              .toDoubleArray(new double[collection.size()]));
        } else {
          parent.add(collection.toArray());
        }
      } else {
        parent.add(collection);
      }
    }

    private Collection<T> newCollection() {
      if (repeatedClass != null) {
        if (repeatedClass.isArray()) {
          // get a collection type that is backed by the correct element array
          if (elementClass.equals(boolean.class)) {
            return (Collection<T>) new BooleanArrayList();
          } else if (elementClass.equals(char.class)) {
            return (Collection<T>) new CharArrayList();
          } else if (elementClass.equals(byte.class)) {
            return (Collection<T>) new ByteArrayList();
          } else if (elementClass.equals(short.class)) {
            return (Collection<T>) new ShortArrayList();
          } else if (elementClass.equals(int.class)) {
            return (Collection<T>) new IntArrayList();
          } else if (elementClass.equals(long.class)) {
            return (Collection<T>) new LongArrayList();
          } else if (elementClass.equals(float.class)) {
            return (Collection<T>) new FloatArrayList();
          } else if (elementClass.equals(double.class)) {
            return (Collection<T>) new DoubleArrayList();
          } else { // Object[]
            return new ArrayList<T>();
          }
        } else if (repeatedClass.isAssignableFrom(ArrayList.class)) {
          return new ArrayList<T>();
        } else {
          // this is non-null when using specific or reflect
          return (Collection<T>) SpecificData.newInstance(repeatedClass, avroSchema);
        }
      }
      // using GenericData or couldn't find the class
      return new GenericData.Array<T>(0, avroSchema);
    }
  }

  static final class AvroUnionConverter<T> extends GroupConverter {

    private final ParentValueContainer parent;
    private final Converter[] memberConverters;
    private Object memberValue = null;

    public AvroUnionConverter(ParentValueContainer parent, Type parquetSchema,
                              GenericData model, Schema avroSchema) {
      this.parent = parent;
      GroupType parquetGroup = parquetSchema.asGroupType();
      this.memberConverters = new Converter[ parquetGroup.getFieldCount()];

      int parquetIndex = 0;
      for (int index = 0; index < avroSchema.getTypes().size(); index++) {
        Schema memberSchema = avroSchema.getTypes().get(index);
        if (!memberSchema.getType().equals(Schema.Type.NULL)) {
          Type memberType = parquetGroup.getType(parquetIndex);
          memberConverters[parquetIndex] = newConverter(model, memberSchema, memberType, new ParentValueContainer() {
            @Override
            void add(Object value) {
              Preconditions.checkArgument(memberValue==null, "Union is resolving to more than one type");
              memberValue = value;
            }
          });
          parquetIndex++; // Note for nulls the parquetIndex id not increased
        }
      }
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return memberConverters[fieldIndex];
    }

    @Override
    public void start() {
      memberValue = null;
    }

    @Override
    public void end() {
      parent.add(memberValue);
    }
  }

  static final class MapConverter<V> extends GroupConverter {

    private final ParentValueContainer parent;
    private final Converter keyValueConverter;
    private Map<String, V> map;

    public MapConverter(ParentValueContainer parent, Type parquetSchema,
        GenericData model, Schema avroSchema) {
      this.parent = parent;
      this.keyValueConverter = new MapKeyValueConverter(parquetSchema, model, avroSchema);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return keyValueConverter;
    }

    @Override
    public void start() {
      this.map = new HashMap<String, V>();
    }

    @Override
    public void end() {
      parent.add(map);
    }

    final class MapKeyValueConverter extends GroupConverter {

      private String key;
      private V value;
      private final Converter keyConverter;
      private final Converter valueConverter;

      public MapKeyValueConverter(Type parquetSchema, GenericData model, Schema avroSchema) {
        keyConverter = new PrimitiveConverter() {
          @Override
          final public void addBinary(Binary value) {
            key = value.toStringUsingUTF8();
          }
        };

        Type valueType = parquetSchema.asGroupType().getType(0).asGroupType().getType(1);
        Schema valueSchema = avroSchema.getValueType();
        valueConverter = newConverter(model, valueSchema, valueType, new ParentValueContainer() {
          @Override
          @SuppressWarnings("unchecked")
          void add(Object value) {
            MapKeyValueConverter.this.value = (V) value;
          }
        });
      }

      @Override
      public Converter getConverter(int fieldIndex) {
        if (fieldIndex == 0) {
          return keyConverter;
        } else if (fieldIndex == 1) {
          return valueConverter;
        }
        throw new IllegalArgumentException("only the key (0) and value (1) fields expected: " + fieldIndex);
      }

      @Override
      public void start() {
        key = null;
        value = null;
      }

      @Override
      public void end() {
        map.put(key, value);
      }
    }
  }

}
