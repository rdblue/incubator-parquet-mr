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

import java.io.IOException;

import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import parquet.filter.UnboundRecordFilter;
import parquet.hadoop.ParquetReader;

/**
 * Read Avro records from a Parquet file.
 */
public class AvroParquetReader<T> extends ParquetReader<T> {

  public AvroParquetReader(Path file) throws IOException {
    super(file, new AvroReadSupport<T>());
  }

  public AvroParquetReader(Path file, GenericData dataModel) throws IOException {
    super(file, new AvroReadSupport<T>(dataModel));
  }

  public AvroParquetReader(Path file, UnboundRecordFilter recordFilter) throws IOException {
    super(file, new AvroReadSupport<T>(), recordFilter);
  }

  public AvroParquetReader(Path file, UnboundRecordFilter recordFilter, GenericData dataModel) throws IOException {
    super(file, new AvroReadSupport<T>(dataModel), recordFilter);
  }

  public AvroParquetReader(Configuration conf, Path file) throws IOException {
    super(conf, file, new AvroReadSupport<T>());
  }

  public AvroParquetReader(Configuration conf, Path file, GenericData dataModel) throws IOException {
    super(conf, file, new AvroReadSupport<T>(dataModel));
  }

  public AvroParquetReader(Configuration conf, Path file, UnboundRecordFilter recordFilter ) throws IOException {
    super(conf, file, new AvroReadSupport<T>(), recordFilter);
  }
}
