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

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.apache.hadoop.fs.Path;
import org.junit.Test;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.metadata.CompressionCodecName;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;

public class TestReflectReadWrite {

  @Test
  public void testReadWriteReflect() throws IOException {
    Path path = writePojosToParquetFile(10, CompressionCodecName.UNCOMPRESSED, false);
    ParquetReader<Pojo> reader = new AvroParquetReader<Pojo>(path);
    Pojo object = getPojo();
    for (int i = 0; i < 10; i++) {
      assertEquals(object, reader.read());
    }
    assertNull(reader.read());
  }

  private Pojo getPojo() {
    Pojo object = new Pojo();
    object.myboolean = true;
    object.mybyte = 1;
    object.myshort = 1;
    object.myint = 1;
    object.mylong = 2L;
    object.myfloat = 3.1f;
    object.mydouble = 4.1;
    object.mybytes = new byte[] { 1, 2, 3, 4 };
    object.mystring = "Hello";
    object.myenum = E.A;
    Map<String, String> map = new HashMap<String, String>();
    map.put("a", "1");
    map.put("b", "2");
    object.mymap = map;
    object.myshortarray = new short[] { 1, 2 };
    object.myintarray = new int[] { 1, 2 };
    object.mystringarray = new String[] { "a", "b" };
    object.mylist = Lists.newArrayList("a", "b", "c");
    return object;
  }

  private Path writePojosToParquetFile( int num, CompressionCodecName compression,
      boolean enableDictionary) throws IOException {
    File tmp = File.createTempFile(getClass().getSimpleName(), ".tmp");
    tmp.deleteOnExit();
    tmp.delete();
    Path path = new Path(tmp.getPath());

    Pojo object = getPojo();

    Schema schema = ReflectData.get().getSchema(object.getClass());
    ParquetWriter<Pojo> writer = new AvroParquetWriter<Pojo>(path, schema, compression,
        DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, enableDictionary);
    for (int i = 0; i < num; i++) {
      writer.write(object);
    }
    writer.close();
    return path;
  }

  public static enum E {
    A, B
  }

  public static class Pojo {
    public boolean myboolean;
    public byte mybyte;
    public short myshort;
    // no char until https://issues.apache.org/jira/browse/AVRO-1458 is fixed
    public int myint;
    public long mylong;
    public float myfloat;
    public double mydouble;
    public byte[] mybytes;
    public String mystring;
    public E myenum;
    private Map<String, String> mymap;
    private short[] myshortarray;
    private int[] myintarray;
    private String[] mystringarray;
    private List<String> mylist;

    @Override
    public boolean equals(Object o) {
      if (!(o instanceof Pojo)) return false;
      Pojo that = (Pojo) o;
      return myboolean == that.myboolean
          && mybyte == that.mybyte
          && myshort == that.myshort
          && myint == that.myint
          && mylong == that.mylong
          && myfloat == that.myfloat
          && mydouble == that.mydouble
          && Arrays.equals(mybytes, that.mybytes)
          && mystring.equals(that.mystring)
          && myenum == that.myenum
          && mymap.equals(that.mymap)
          && Arrays.equals(myshortarray, that.myshortarray)
          && Arrays.equals(myintarray, that.myintarray)
          && Arrays.equals(mystringarray, that.mystringarray)
          && mylist.equals(that.mylist);
    }

    @Override
    public String toString() {
      return "Pojo{" +
          "myboolean=" + myboolean +
          ", mybyte=" + mybyte +
          ", myshort=" + myshort +
          ", myint=" + myint +
          ", mylong=" + mylong +
          ", myfloat=" + myfloat +
          ", mydouble=" + mydouble +
          ", mybytes=" + Arrays.toString(mybytes) +
          ", mystring='" + mystring + '\'' +
          ", myenum=" + myenum +
          ", mymap=" + mymap +
          ", myshortarray=" + Arrays.toString(myshortarray) +
          ", myintarray=" + Arrays.toString(myintarray) +
          ", mystringarray=" + Arrays.toString(mystringarray) +
          ", mylist=" + mylist +
          '}';
    }
  }

}
