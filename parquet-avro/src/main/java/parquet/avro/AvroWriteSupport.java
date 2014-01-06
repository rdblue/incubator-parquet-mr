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

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import parquet.hadoop.api.WriteSupport;
import parquet.io.api.Binary;
import parquet.io.api.RecordConsumer;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.Type;

/**
 * Avro implementation of {@link WriteSupport} for Avro records,
 * which covers Avro Generic, Specific, and Reflect models.
 * Users should use {@link AvroParquetWriter} or {@link AvroParquetOutputFormat} rather than using
 * this class directly.
 */
public class AvroWriteSupport<T> extends WriteSupport<T> {

  private static final String AVRO_SCHEMA = "parquet.avro.schema";

  private RecordConsumer recordConsumer;
  private MessageType rootSchema;
  private Schema rootAvroSchema;
  private ReflectData data = ReflectData.get();

  public AvroWriteSupport() {
  }

  public AvroWriteSupport(MessageType schema, Schema avroSchema) {
    this.rootSchema = schema;
    this.rootAvroSchema = avroSchema;
  }

  /**
   * @see parquet.avro.AvroParquetOutputFormat#setSchema(org.apache.hadoop.mapreduce.Job, org.apache.avro.Schema)
   */
  public static void setSchema(Configuration configuration, Schema schema) {
    configuration.set(AVRO_SCHEMA, schema.toString());
  }

  @Override
  public WriteContext init(Configuration configuration) {
    if (rootAvroSchema == null) {
      rootAvroSchema = new Schema.Parser().parse(configuration.get(AVRO_SCHEMA));
      rootSchema = new AvroSchemaConverter().convert(rootAvroSchema);
    }
    Map<String, String> extraMetaData = new HashMap<String, String>();
    extraMetaData.put(AvroReadSupport.AVRO_SCHEMA_METADATA_KEY, rootAvroSchema.toString());
    return new WriteContext(rootSchema, extraMetaData);
  }

  @Override
  public void prepareForWrite(RecordConsumer recordConsumer) {
    this.recordConsumer = recordConsumer;
  }

  @SuppressWarnings("unchecked")
  // overloaded version for backward compatibility
  public void write(IndexedRecord record) {
    recordConsumer.startMessage();
    writeRecordFields(rootSchema, rootAvroSchema, (T) record);
    recordConsumer.endMessage();
  }

  @Override
  public void write(T record) {
    recordConsumer.startMessage();
    writeRecordFields(rootSchema, rootAvroSchema, record);
    recordConsumer.endMessage();
  }

  private void writeRecord(GroupType schema, Schema avroSchema,
                           T record) {
    recordConsumer.startGroup();
    writeRecordFields(schema, avroSchema, record);
    recordConsumer.endGroup();
  }

  private void writeRecordFields(GroupType schema, Schema avroSchema,
                                 T record) {
    List<Type> fields = schema.getFields();
    List<Schema.Field> avroFields = avroSchema.getFields();
    int index = 0; // parquet ignores Avro nulls, so index may differ
    for (int avroIndex = 0; avroIndex < avroFields.size(); avroIndex++) {
      Schema.Field avroField = avroFields.get(avroIndex);
      if (avroField.schema().getType().equals(Schema.Type.NULL)) {
        continue;
      }
      Type fieldType = fields.get(index);
      Object value = data.getField(record, avroField.name(), avroIndex);
      if (value != null) {
        recordConsumer.startField(fieldType.getName(), index);
        writeValue(fieldType, avroField.schema(), value);
        recordConsumer.endField(fieldType.getName(), index);
      } else if (fieldType.isRepetition(Type.Repetition.REQUIRED)) {
        throw new RuntimeException("Null-value for required field: " + avroField.name());
      }
      index++;
    }
  }

  private void writeArray(GroupType schema, Schema avroSchema, Object value) {
    if (value instanceof Collection) {
      Iterable<?> array = (Iterable<?>) value;
      recordConsumer.startGroup(); // group wrapper (original type LIST)
      if (array.iterator().hasNext()) {
        recordConsumer.startField("array", 0);
        for (Object elt : array) {
          writeValue(schema.getType(0), avroSchema.getElementType(), elt);
        }
        recordConsumer.endField("array", 0);
      }
      recordConsumer.endGroup();
      return;
    }
    Class<?> elementClass = value.getClass().getComponentType();
    if (elementClass == null) {
      // not a Collection or an Array
      throw new AvroTypeException("Array data must be a Collection or Array");
    }
    recordConsumer.startGroup(); // group wrapper (original type LIST)
    if (elementClass.isPrimitive()) {
      Schema.Type type = avroSchema.getElementType().getType();
      switch(type) {
        case BOOLEAN: {
          boolean[] array = (boolean[]) value;
          if (array.length > 0) {
            recordConsumer.startField("array", 0);
            for (boolean elt : array) {
              recordConsumer.addBoolean(elt);
            }
            recordConsumer.endField("array", 0);
          }
          break;
        }
        case INT: {
          if(elementClass.equals(int.class)) {
            int[] array = (int[]) value;
            if (array.length > 0) {
              recordConsumer.startField("array", 0);
              for (int elt : array) {
                recordConsumer.addInteger(elt);
              }
              recordConsumer.endField("array", 0);
            }
          } else if(elementClass.equals(char.class)) {
            char[] array = (char[]) value;
            if (array.length > 0) {
              recordConsumer.startField("array", 0);
              for (char elt : array) {
                recordConsumer.addInteger(elt);
              }
              recordConsumer.endField("array", 0);
            }
          } else if(elementClass.equals(short.class)) {
            short[] array = (short[]) value;
            if (array.length > 0) {
              recordConsumer.startField("array", 0);
              for (short elt : array) {
                recordConsumer.addInteger(elt);
              }
              recordConsumer.endField("array", 0);
            }
          } else {
            arrayError(elementClass, type);
          }
          break;
        }
        case LONG: {
          long[] array = (long[]) value;
          if (array.length > 0) {
            recordConsumer.startField("array", 0);
            for (long elt : array) {
              recordConsumer.addLong(elt);
            }
            recordConsumer.endField("array", 0);
          }
          break;
        }
        case FLOAT: {
          float[] array = (float[]) value;
          if (array.length > 0) {
            recordConsumer.startField("array", 0);
            for (float elt : array) {
              recordConsumer.addFloat(elt);
            }
            recordConsumer.endField("array", 0);
          }
          break;
        }
        case DOUBLE: {
          double[] array = (double[]) value;
          if (array.length > 0) {
            recordConsumer.startField("array", 0);
            for (double elt : array) {
              recordConsumer.addDouble(elt);
            }
            recordConsumer.endField("array", 0);
          }
          break;
        }
        default:
          arrayError(elementClass, type);
      }
    } else {
      Object[] array = (Object[]) value;
      if (array.length > 0) {
        recordConsumer.startField("array", 0);
        for (Object elt : array) {
          writeValue(schema.getType(0), avroSchema.getElementType(), elt);
        }
        recordConsumer.endField("array", 0);
      }
    }
    recordConsumer.endGroup();
  }

  private void arrayError(Class<?> cl, Schema.Type type) {
    throw new AvroTypeException("Error writing array with inner type " +
        cl + " and avro type: " + type);
  }

  private <V> void writeMap(GroupType schema, Schema avroSchema, 
                            Map<CharSequence, V> map) {
    GroupType innerGroup = schema.getType(0).asGroupType();
    Type keyType = innerGroup.getType(0);
    Type valueType = innerGroup.getType(1);
    Schema keySchema = Schema.create(Schema.Type.STRING);

    recordConsumer.startGroup(); // group wrapper (original type MAP)
    if (map.size() > 0) {
      recordConsumer.startField("map", 0);
      recordConsumer.startGroup(); // "repeated" group wrapper
      recordConsumer.startField("key", 0);
      for (CharSequence key : map.keySet()) {
        writeValue(keyType, keySchema, key);
      }
      recordConsumer.endField("key", 0);
      recordConsumer.startField("value", 1);
      for (V value : map.values()) {
        writeValue(valueType, avroSchema.getValueType(), value);
      }
      recordConsumer.endField("value", 1);
      recordConsumer.endGroup();
      recordConsumer.endField("map", 0);
    }
    recordConsumer.endGroup();
  }

  private void writeUnion(GroupType parquetSchema, Schema avroSchema, 
                          Object value) {
    recordConsumer.startGroup();

    // ResolveUnion will tell us which of the union member types to 
    // deserialise.
    int avroIndex = GenericData.get().resolveUnion(avroSchema, value);

    // For parquet's schema we skip nulls
    GroupType parquetGroup = parquetSchema.asGroupType();
    int parquetIndex = avroIndex;
    for (int i = 0; i < avroIndex; i++) {
      if (avroSchema.getTypes().get(i).getType().equals(Schema.Type.NULL)) {
        parquetIndex--;
      }
    }

    // Sparsely populated method of encoding unions, each member has its own 
    // set of columns.
    String memberName = "member" + parquetIndex;
    recordConsumer.startField(memberName, parquetIndex);
    writeValue(parquetGroup.getType(parquetIndex), 
               avroSchema.getTypes().get(avroIndex), value);
    recordConsumer.endField(memberName, parquetIndex);

    recordConsumer.endGroup();
  }

  @SuppressWarnings("unchecked")
  private void writeValue(Type type, Schema avroSchema, Object value) {
    Schema nonNullAvroSchema = AvroSchemaConverter.getNonNull(avroSchema);
    Schema.Type avroType = nonNullAvroSchema.getType();
    if (avroType.equals(Schema.Type.BOOLEAN)) {
      recordConsumer.addBoolean((Boolean) value);
    } else if (avroType.equals(Schema.Type.INT)) {
      if (value instanceof Character) {
        recordConsumer.addInteger((int) ((Character) value).charValue());
      } else {
        recordConsumer.addInteger(((Number) value).intValue());
      }
    } else if (avroType.equals(Schema.Type.LONG)) {
      recordConsumer.addLong(((Number) value).longValue());
    } else if (avroType.equals(Schema.Type.FLOAT)) {
      recordConsumer.addFloat(((Number) value).floatValue());
    } else if (avroType.equals(Schema.Type.DOUBLE)) {
      recordConsumer.addDouble(((Number) value).doubleValue());
    } else if (avroType.equals(Schema.Type.BYTES)) {
      if (value instanceof byte[]) {
        recordConsumer.addBinary(Binary.fromByteArray((byte[]) value));
      } else {
        recordConsumer.addBinary(Binary.fromByteBuffer((ByteBuffer) value));
      }
    } else if (avroType.equals(Schema.Type.STRING)) {
      recordConsumer.addBinary(fromAvroString(value));
    } else if (avroType.equals(Schema.Type.RECORD)) {
      writeRecord((GroupType) type, nonNullAvroSchema, (T) value);
    } else if (avroType.equals(Schema.Type.ENUM)) {
      recordConsumer.addBinary(Binary.fromString(value.toString()));
    } else if (avroType.equals(Schema.Type.ARRAY)) {
      writeArray((GroupType) type, nonNullAvroSchema, value);
    } else if (avroType.equals(Schema.Type.MAP)) {
      writeMap((GroupType) type, nonNullAvroSchema, (Map<CharSequence, ?>) value);
    } else if (avroType.equals(Schema.Type.UNION)) {
      writeUnion((GroupType) type, nonNullAvroSchema, value);
    } else if (avroType.equals(Schema.Type.FIXED)) {
      recordConsumer.addBinary(Binary.fromByteArray(((GenericFixed) value).bytes()));
    }
  }

  private Binary fromAvroString(Object value) {
    if (value instanceof Utf8) {
      Utf8 utf8 = (Utf8) value;
      return Binary.fromByteArray(utf8.getBytes(), 0, utf8.getByteLength());
    }
    return Binary.fromString(value.toString());
  }

}
