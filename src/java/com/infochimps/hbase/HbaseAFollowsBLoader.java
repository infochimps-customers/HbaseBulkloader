package com.infochimps.hbase;

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
import org.apache.hadoop.hbase.mapreduce.KeyValueSortReducer;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
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
import com.infochimps.hadoop.BenfordAndSonPartitioner;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.util.ToolRunner;


/* This class is specifically desinged to load the data from
 * a_atsigns_b into an a_rel_b hbase table with the following format:
 * 
 * rowkey: <user_a_id>:<user_b_id>
 * families:   follow:ab->""
 *             follow:ba->""
 */      
public class HbaseAFollowsBLoader extends Configured implements Tool {

    private final static Log LOG = LogFactory.getLog(HbaseAFollowsBLoader.class);
    public static String HBASE_TABLE_NAME   = "hbase.table.name";

    public static class HbaseAFollowsBLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue> {
 
        private long checkpoint = 10000;
        private long count = 0;

	private byte[] family;
	private byte[] ab;
	private byte[] ba;
	private byte[] value;

	static enum Problems { MISSING_A_ID, MISSING_B_ID, MISSING_RELATIONSHIP, MISSING_TWEET_ID, BAD_RECORD };

        @Override
            public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration config = context.getConfiguration();
	    // Set up some constants
	    family = Bytes.toBytes("follow");
	    ab     = Bytes.toBytes("ab");
	    ba     = Bytes.toBytes("ba");
	    value  = Bytes.toBytes("");
        }


        @Override
        public void map(LongWritable key, Text line, Context context) throws IOException {
            String[] fields = line.toString().split("\t");
	   
            StringBuffer keyab = new StringBuffer();
	    StringBuffer keyba = new StringBuffer();
           	    
	    // If there is anything wrong with the record, we will throw an ArrayIndexOutOfBoundsException
	    // which will be ignored, but we will just move on to the next record.
            try {
		// skip bad records that do not have rectified user ids
		if( fields[1].length() == 0 ) {
		    context.getCounter(Problems.MISSING_A_ID).increment(1);
		    throw new ArrayIndexOutOfBoundsException();
		}
		if( fields[2].length() == 0) {
		    context.getCounter(Problems.MISSING_B_ID).increment(1);
		    throw new ArrayIndexOutOfBoundsException();
		}

		keyab.append(fields[1]);
		keyab.append(":");
		keyab.append(fields[2]);

		keyba.append(fields[2]);
		keyba.append(":");
		keyba.append(fields[1]);

                byte[] rowkeyab = Bytes.toBytes(keyab.toString());
                byte[] rowkeyba = Bytes.toBytes(keyba.toString());

                // Create KeyValues
                KeyValue kvab = new KeyValue(rowkeyab, family, ab,  System.currentTimeMillis(), value);
                KeyValue kvba = new KeyValue(rowkeyba, family, ba,  System.currentTimeMillis(), value);

                try {
                    context.write(new ImmutableBytesWritable(rowkeyab), kvab);
                    context.write(new ImmutableBytesWritable(rowkeyba), kvba);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // Set status every checkpoint lines
                if (++count % checkpoint == 0) {
                    context.setStatus("Emitting KeyValue: " + count + " - " + Bytes.toString(rowkeyab));
                }
            } catch (ArrayIndexOutOfBoundsException e) {
		context.getCounter(Problems.BAD_RECORD).increment(1);
            }
        }
    }
        
    public int run(String[] args) throws Exception {
       Job job  = new Job(getConf());

        // Set job class and job name
        job.setJarByClass(HbaseAFollowsBLoader.class);
        job.setJobName("HbaseAFollowsBLoader");

        job.setMapOutputKeyClass(ImmutableBytesWritable.class);
        job.setMapOutputValueClass(KeyValue.class);

        job.setMapperClass(HbaseAFollowsBLoadMapper.class);
        job.setReducerClass(KeyValueSortReducer.class);
        job.setOutputFormatClass(HFileOutputFormat.class);

        // We will almost certainly want to use a different partitioner
        job.setPartitionerClass(BenfordAndSonPartitioner.class);
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
	Configuration config = HBaseConfiguration.create();
        int res = ToolRunner.run(config, new HbaseAFollowsBLoader(), args);
        System.exit(res);
    }
}
