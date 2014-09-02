package parquet.scrooge;

import au.com.cba.omnia.ebenezer.example.Customer;
import java.io.IOException;
import java.lang.reflect.Constructor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RunningJob;
import org.junit.Test;
import parquet.hadoop.ParquetInputFormat;
import parquet.hadoop.mapred.Container;
import parquet.hadoop.mapred.DeprecatedParquetInputFormat;
import parquet.hadoop.thrift.ParquetThriftInputFormat;
import parquet.hadoop.thrift.ThriftReadSupport;

public class TestParquetScroogeScheme {

  public static class DeprecatedWriteMapper implements org.apache.hadoop.mapred.Mapper<Void, Container<Object>, LongWritable, Text> {

    @Override
    public void map(Void aVoid, Container<Object> valueContainer, OutputCollector<LongWritable, Text> longWritableTextOutputCollector, Reporter reporter) throws IOException {
      longWritableTextOutputCollector.collect(null, new Text(valueContainer.get().toString()));
      System.err.println(valueContainer.get().toString());
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public void configure(JobConf entries) {
    }
  }

  @Test
  public void testCustomerRead() throws Exception {
    Configuration conf = new Configuration();

    final JobConf jobConf = new JobConf(conf);

    jobConf.setInputFormat(DeprecatedParquetInputFormat.class);
    ParquetInputFormat.setReadSupportClass(jobConf, ThriftReadSupport.class);
    ThriftReadSupport.setRecordConverterClass(jobConf, ScroogeRecordConverter.class);
    ParquetThriftInputFormat.setThriftClass(jobConf, Customer.class);
    DeprecatedParquetInputFormat.setInputPaths(jobConf, new Path("/home/blue/tmp/CDH-21096.parquet"));

    {
      jobConf.setOutputFormat(org.apache.hadoop.mapred.TextOutputFormat.class);
      org.apache.hadoop.mapred.TextOutputFormat.setOutputPath(jobConf, new Path("target/output"));
      jobConf.setMapperClass(DeprecatedWriteMapper.class);
      jobConf.setNumReduceTasks(0);
      RunningJob mapRedJob = JobClient.runJob(jobConf);
      mapRedJob.waitForCompletion();
    }

    DeprecatedParquetInputFormat inputFormat = new DeprecatedParquetInputFormat();
    InputSplit[] splits = inputFormat.getSplits(jobConf, 2);
    Class<? extends InputSplit> splitClass = splits[0].getClass();
    Constructor<? extends InputSplit> ctor = splitClass.getConstructor();
    ctor.setAccessible(true);
    DataOutputBuffer out = new DataOutputBuffer();
    for (InputSplit split : splits) {
      split.write(out);
    }
    DataInputBuffer in = new DataInputBuffer();
    in.reset(out.getData(), out.getLength());
    InputSplit[] newSplits = new InputSplit[splits.length];
    for (int i = 0; i < splits.length; i += 1) {
      newSplits[i] = ctor.newInstance();
      newSplits[i].readFields(in);
    }
  }
}
