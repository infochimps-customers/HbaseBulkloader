package com.infochimps.hbase;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.ToolRunner;

import com.google.common.base.Function;
import org.apache.hadoop.hbase.KeyValue;
/*
 * Sample uploader.
 * 
 * This is EXAMPLE code.  You will need to change it to work for your context.
 * 
 * Uses TableReduce to put the data into hbase. Change the InputFormat to suit
 * your data. Use the map to massage the input so it fits hbase.  Currently its
 * just a pass-through map.  In the reduce, you need to output a row and a
 * map of columns to cells.  Change map and reduce to suit your input.
 * 
 * <p>The below is wired up to handle an input whose format is a text file
 * which has a line format as follow:
 * <pre>
 * row columnname columndata
 * </pre>
 * 
 * <p>The table and columnfamily we're to insert into must preexist.
 * 
 * <p>Do the following to start the MR job:
 * <pre>
 * ./bin/hadoop org.apache.hadoop.hbase.mapred.SampleUploader /tmp/input.txt TABLE_NAME
 * </pre>
 * 
 * <p>This code was written against hbase 0.1 branch.
 */

public class HbaseGraphTableBulkLoader extends Configured implements Tool {

    // configuration parameters
    // hbase.table.name should contain the name of the table
    public static String HBASE_TABLE_NAME   = "hbase.table.name";
    // hbase.family.name should contain the name of the column family
    public static String HBASE_FAMILY_NAME  = "hbase.family.name";
    // hbase.column.names should contain a comma delimited list of strings
    // to be used as the column names (aka qualifiers) for the data inserts
    public static String HBASE_COLUMN_NAMES = "hbase.column.names";
    // hbase.key.index should contain a zero based index of the column to be used
    // as the row key
    public static String HBASE_KEY_INDEXES    = "hbase.key.indexes";

    public static class HbaseGraphTableLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        private byte[][] fieldNameBytes;
        private byte[] family;
        private int[] keyFieldIndexes;

        private long checkpoint = 1000;
        private long count = 0;

        @Override
        public void setup(Context context) {
            Configuration config = context.getConfiguration();
            String[] fieldNames = config.getStrings(HBASE_COLUMN_NAMES);
            fieldNameBytes = new byte[fieldNames.length][];
            String[] keys = config.getStrings(HBASE_KEY_INDEXES);
            keyFieldIndexes = new int[keys.length];
            for(int i=0;i<keys.length;++i) {
                keyFieldIndexes[i] = Integer.parseInt(keys[i]);
            }
            for(int i=0;i<fieldNames.length;++i) {
                if(fieldNames[i].equals("-"))
                    fieldNameBytes[i] = null;
                else
                    fieldNameBytes[i] = Bytes.toBytes(fieldNames[i]);
            }
            family = Bytes.toBytes( config.get(HBASE_FAMILY_NAME));
        }

        @Override
        public void map(LongWritable key, Text line, Context context) throws IOException {
            String[] fields = line.toString().split("\t");

            StringBuffer keybuf = new StringBuffer();
            try {
                // build key out of keyfields separated by ":"
                // all keyfieds must be non-empty, or we skip the recored
                for (int i = 0; i < keyFieldIndexes.length; ++i) {
                    if( fields[keyFieldIndexes[i]].length() == 0) {
                        // TODO - define a more appropriate exection
                        throw new ArrayIndexOutOfBoundsException();
                    }
                    if (i > 0) {
                        keybuf.append(":");
                    }
                    keybuf.append(fields[keyFieldIndexes[i]]);
                }

                byte[] rowkey = Bytes.toBytes(keybuf.toString());

                // Create Put
                Put put = new Put(rowkey);

                for (int i = 0; i < fields.length && i < fieldNameBytes.length; ++i) {
                    // skip name/value if the name was given as a "-"
                    if (fieldNameBytes[i] == null) {
                        continue;
                    }
                    // skip name/value if the value is empty
                    if (fields[i].length() < 1) {
                        continue;
                    }
                    put.add(family, fieldNameBytes[i], Bytes.toBytes(fields[i]));
                }

                // Uncomment below to disable WAL. This will improve performance but means
                // you will experience data loss in the case of a RegionServer crash.

                try {
                    context.write(new ImmutableBytesWritable(rowkey), put);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // Set status every checkpoint lines
                if (++count % checkpoint == 0) {
                    context.setStatus("Emitting Put: " + count + " - " + Bytes.toString(rowkey));
                }
            } catch (ArrayIndexOutOfBoundsException e) {
                // TODO: increment a counter or something
            }
        }
    }
        
    public int run(String[] args) throws Exception {
       Job job  = new Job(getConf());

        // Set job class and job name
        job.setJarByClass(HbaseGraphTableBulkLoader.class);
        job.setJobName("HbaseGraphTableBulkLoader");

        // Set mapper class and reducer class
        job.setMapperClass(HbaseGraphTableLoadMapper.class);
        job.setNumReduceTasks(0);

        // Hbase specific setup
        Configuration conf = job.getConfiguration();
        TableMapReduceUtil.initTableReducerJob(conf.get( HBASE_TABLE_NAME ), null, job);

        // Handle input path
        List<String> other_args = new ArrayList<String>();
        for (int i=0; i < args.length; ++i) {
            other_args.add(args[i]);
        }
        FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));

        // Submit job to server and wait for completion
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(String[] args) throws Exception {
	Configuration config = HBaseConfiguration.create();
        int res = ToolRunner.run(config, new HbaseGraphTableBulkLoader(), args);
        System.exit(res);
    }
}
