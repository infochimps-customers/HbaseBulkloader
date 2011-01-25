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
import org.apache.hadoop.mapreduce.Counter;
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

/* This class is specifically desinged to load the data from
 * a_atsigns_b into an a_rel_b hbase table with the following format:
 * 
 * rowkey: <user_a_id>:<user_b_id>
 * families:   follow:ab->""
 *             follow:ba->""
 */      
public class HbaseAFollowsBLoader extends Configured implements Tool {

    // configuration parameters
    // hbase.table.name should contain the name of the table
    public static String HBASE_TABLE_NAME   = "hbase.table.name";

    public static class HbaseAFollowsBLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
 
        private long checkpoint = 1000;
        private long count = 0;

	private byte[] family;
	private byte[] ab;
	private byte[] ba;
	private byte[] value;

	static enum Problems { MISSING_A_ID, MISSING_B_ID, MISSING_RELATIONSHIP, MISSING_TWEET_ID, BAD_RECORD };

        @Override
        public void setup(Context context) {
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
           	    
	    // If there is anything wron with the record, we will throw an ArrayIndexOutOfBoundsException
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

                // Create Puts
                Put putab = new Put(rowkeyab);
                Put putba = new Put(rowkeyba);

		putab.add(family, ab, value);
		putba.add(family, ba, value);
       
                // Uncomment below to disable WAL. This will improve performance but means
                // you will experience data loss in the case of a RegionServer crash.
		putab.setWriteToWAL(false);
		putba.setWriteToWAL(false);

                try {
                    context.write(new ImmutableBytesWritable(rowkeyab), putab);
                    context.write(new ImmutableBytesWritable(rowkeyba), putba);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // Set status every checkpoint lines
                if (++count % checkpoint == 0) {
                    context.setStatus("Emitting Put: " + count + " - " + Bytes.toString(rowkeyab));
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

        // Set mapper class and reducer class
        job.setMapperClass(HbaseAFollowsBLoadMapper.class);
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
        int res = ToolRunner.run(config, new HbaseAFollowsBLoader(), args);
        System.exit(res);
    }
}
