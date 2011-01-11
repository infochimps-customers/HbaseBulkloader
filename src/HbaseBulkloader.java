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

public class HbaseBulkloader extends Configured implements Tool {
    
    public static class HbaseLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
        
        public void map(LongWritable key, Text line, Context context) throws IOException {
        }
    }
        
    public int run(String[] args) throws Exception {

        Job job  = new Job(getConf());
        
        // Set job class and job name
        job.setJarByClass(HbaseBulkloader.class);
        job.setJobName("HbaseBulkloader");

        // Set mapper class and reducer class
        job.setMapperClass(HbaseLoadMapper.class);
        job.setNumReduceTasks(0);

        // Hbase specific setup
        Configuration conf = job.getConfiguration();
        TableMapReduceUtil.initTableReducerJob(conf.get("hbase.table.name"), null, job);
        //

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
        int res = ToolRunner.run(new HBaseConfiguration(), new HbaseBulkloader(), args);
        System.exit(res);
    }
}

// public class SampleUploader {
// 
//   private static final String NAME = "SampleUploader";
//   
//   static class Uploader 
//   extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {
// 
//     private long checkpoint = 100;
//     private long count = 0;
//     
//     @Override
//     public void map(LongWritable key, Text line, Context context)
//     throws IOException {
//       
//       // Input is a CSV file
//       // Each map() is a single line, where the key is the line number
//       // Each line is comma-delimited; row,family,qualifier,value
//             
//       // Split CSV line
//       String [] values = line.toString().split(",");
//       if(values.length != 4) {
//         return;
//       }
//       
//       // Extract each value
//       byte [] row = Bytes.toBytes(values[0]);
//       byte [] family = Bytes.toBytes(values[1]);
//       byte [] qualifier = Bytes.toBytes(values[2]);
//       byte [] value = Bytes.toBytes(values[3]);
//       
//       // Create Put
//       Put put = new Put(row);
//       put.add(family, qualifier, value);
//       
//       // Uncomment below to disable WAL. This will improve performance but means
//       // you will experience data loss in the case of a RegionServer crash.
//       // put.setWriteToWAL(false);
//       
//       try {
//         context.write(new ImmutableBytesWritable(row), put);
//       } catch (InterruptedException e) {
//         e.printStackTrace();
//       }
//       
//       // Set status every checkpoint lines
//       if(++count % checkpoint == 0) {
//         context.setStatus("Emitting Put " + count);
//       }
//     }
//   }
//   
//   /**
//    * Job configuration.
//    */
//   public static Job configureJob(Configuration conf, String [] args)
//   throws IOException {
//     Path inputPath = new Path(args[0]);
//     String tableName = args[1];
//     Job job = new Job(conf, NAME + "_" + tableName);
//     job.setJarByClass(Uploader.class);
//     FileInputFormat.setInputPaths(job, inputPath);
//     job.setInputFormatClass(SequenceFileInputFormat.class);
//     job.setMapperClass(Uploader.class);
//     // No reducers.  Just write straight to table.  Call initTableReducerJob
//     // because it sets up the TableOutputFormat.
//     TableMapReduceUtil.initTableReducerJob(tableName, null, job);
//     job.setNumReduceTasks(0);
//     return job;
//   }
// 
//   /**
//    * Main entry point.
//    * 
//    * @param args  The command line parameters.
//    * @throws Exception When running the job fails.
//    */
//   public static void main(String[] args) throws Exception {
//     HBaseConfiguration conf = new HBaseConfiguration();
//     String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
//     if(otherArgs.length != 2) {
//       System.err.println("Wrong number of arguments: " + otherArgs.length);
//       System.err.println("Usage: " + NAME + " <input> <tablename>");
//       System.exit(-1);
//     }
//     Job job = configureJob(conf, otherArgs);
//     System.exit(job.waitForCompletion(true) ? 0 : 1);
//   }
// }
