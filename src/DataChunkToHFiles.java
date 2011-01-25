package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.PerformanceEvaluation;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
// import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;
    
import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.ToolRunner;


public class DataChunkToHFiles extends Configured implements Tool {

    private final static Log LOG = LogFactory.getLog(DataChunkToHFiles.class);
    public static class TextToKeyValues extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
        private byte[] columnFamily;
        private byte[] tableName;
        private int keyField;
        private String[] fieldNames;
      
        @Override
            protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            tableName    = Bytes.toBytes(conf.get("hbase.table.name"));
            columnFamily = Bytes.toBytes(conf.get("hbase.column.family"));
            keyField     = conf.getInt("hbase.key.field", 0); //default to field 0 as the row key
            fieldNames   = conf.get("hbase.field.names").split(",");
        }

        protected void map(LongWritable key, Text line, Context context) throws IOException ,InterruptedException {
            String[] fields = line.toString().split("\t");
            byte[] rowKey   = Bytes.toBytes(fields[keyField]);

            // Create output for Hbase reducer
            ImmutableBytesWritable hbaseRowKey = new ImmutableBytesWritable(rowKey);
          
            for(int i = 0; i < fields.length; i++) {
                if (i < fieldNames.length && i != keyField) {
                    if (fields[i].length() != 0) {
                        byte[] columnName  = Bytes.toBytes(fieldNames[i]);
                        byte[] columnValue = Bytes.toBytes(fields[i]);
                        KeyValue kv = new KeyValue(rowKey, columnFamily, columnName,  System.currentTimeMillis(), columnValue);
                        context.write(hbaseRowKey, kv);
                    }
                }
            }
        }
    }
    public int run(String[] args) throws Exception {

        Job job  = new Job(getConf());
        
        job.setJarByClass(DataChunkToHFiles.class);
        job.setJobName("HFilesWriter, This should really be a Pig StoreFunc");

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);
        
        job.setMapperClass(TextToKeyValues.class);
        job.setReducerClass(KeyValueSortReducer.class);
        job.setOutputFormatClass(HFileOutputFormat.class);
                
        // We will almost certainly want to use a different partitioner
        // job.setPartitionerClass(BenfordAndSonPartitioner.class);
        //
        
        // Try total order partitioner with sampling
        Configuration conf = job.getConfiguration();
        // job.setPartitionerClass(TotalOrderPartitioner.class);
        //
        
        // Handle input path
        List<String> other_args = new ArrayList<String>();
        for (int i=0; i < args.length; ++i) {
            other_args.add(args[i]);
        }
        FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));
        FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));

        // Submit job to server and wait for completion
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DataChunkToHFiles(), args);
        System.exit(res);
    }
}
