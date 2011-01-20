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
 * families:   reply:tweet_id -> "json"
 *           retweet:tweet_id -> "json"
 *           mention:tweet_id -> "json"
 *
 * The table will also have   afollowsb:yes   and bfollowsa:yes if
 * the condition in question has been observed.
 */      
public class HbaseAAtsignsBLoader extends Configured implements Tool {

    // configuration parameters
    // hbase.table.name should contain the name of the table
    public static String HBASE_TABLE_NAME   = "hbase.table.name";

    public static class HbaseAAtsignsBLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
 
        private long checkpoint = 1000;
        private long count = 0;

	static enum Problems { MISSING_A_ID, MISSING_B_ID, MISSING_RELATIONSHIP, MISSING_TWEET_ID, BAD_RECORD };

        @Override
        public void setup(Context context) {
            Configuration config = context.getConfiguration();
        }


        @Override
        public void map(LongWritable key, Text line, Context context) throws IOException {
            String[] fields = line.toString().split("\t");
	   
            StringBuffer keybuf = new StringBuffer();
	    StringBuffer valbuf = new StringBuffer();
           
	    byte[] family;
	    byte[] column;
	    
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

		keybuf.append(fields[1]);
		keybuf.append(":");
		keybuf.append(fields[2]);

                byte[] rowkey = Bytes.toBytes(keybuf.toString());

		// The family will be either "reply", "retweet" or "mention" depending
		// on the value of the fourth field  (ie fields[3])
		if( "re".equals(fields[3]) ) {
		    family = Bytes.toBytes("reply");
		} else if( "rt".equals(fields[3])) {
		    family = Bytes.toBytes("retweet");
		} else if( "me".equals(fields[3])) {
		    family = Bytes.toBytes("mention");
		} else {
		    context.getCounter(Problems.MISSING_RELATIONSHIP).increment(1);
		    throw new ArrayIndexOutOfBoundsException();
		}

		// The column name is just going to be the id of the tweet
		if( fields[4].length() == 0) {
		    context.getCounter(Problems.MISSING_TWEET_ID).increment(1);
		    throw new ArrayIndexOutOfBoundsException();
		}
		column = Bytes.toBytes( fields[4] );
		
		valbuf.append("{\"created_at\":\"");
		valbuf.append(fields[5]);
		valbuf.append("\"");
		if(fields.length > 7) {
		    valbuf.append(",\"rel_tw_id\":\"");
		    valbuf.append(fields[7]);
		    valbuf.append("\"");
		}
		valbuf.append("}");
		
		
                // Create Put
                Put put = new Put(rowkey);
		put.add(family, column, Bytes.toBytes(valbuf.toString()));
       
                // Uncomment below to disable WAL. This will improve performance but means
                // you will experience data loss in the case of a RegionServer crash.
		put.setWriteToWAL(false);

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
		context.getCounter(Problems.BAD_RECORD).increment(1);
                // TODO: increment a counter or something
            }
        }
    }
        
    public int run(String[] args) throws Exception {
       Job job  = new Job(getConf());

        // Set job class and job name
        job.setJarByClass(HbaseAAtsignsBLoader.class);
        job.setJobName("HbaseAAtsignsBLoader");

        // Set mapper class and reducer class
        job.setMapperClass(HbaseAAtsignsBLoadMapper.class);
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
        int res = ToolRunner.run(config, new HbaseAAtsignsBLoader(), args);
        System.exit(res);
    }
}
