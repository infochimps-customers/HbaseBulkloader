package com.infochimps.hbase.pig;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Iterator;
import java.util.TreeSet;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.pig.LoadCaster;
import org.apache.pig.LoadFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.StoreFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Utils;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.common.collect.Lists;

//
// This can work. Note: 1. Need to run a total sort over the whole
// data set. 2. Need to sort the keyvalues before writing. It may be
// that simply calling ORDER with no other arguments in the pig script
// itself will be fine.
//
public class HFileStorage extends StoreFunc {

    protected RecordWriter writer = null;
    private String tableURI;
    private byte[] tableName;
    private byte[] columnFamily;
    private String[] columnNames;
    
    /**
     * Constructor. Construct a HFile StoreFunc to write data out as HFiles. These
     * HFiles will then have to be imported with the hbase/bin/loadtable.rb tool. 
     * @param tN The HBase table name the data will ultimately wind up in. It does not need to exist ahead of time.
     * @param cF The HBase column family name for the table the data will wind up it. It does not need to exist ahead of time.
     * @param columnNames A comma separated list of column names descibing the fields in a tuple.
     */
    public HFileStorage(String tN, String cF, String names) {
        this.tableName    = Bytes.toBytes(tN);
        this.columnFamily = Bytes.toBytes(cF);
        this.columnNames  = names.split(",");
    }

    //
    // getOutputFormat()
    //
    // This method will be called by Pig to get the OutputFormat
    // used by the storer. The methods in the OutputFormat (and
    // underlying RecordWriter and OutputCommitter) will be
    // called by pig in the same manner (and in the same context)
    // as by Hadoop in a map-reduce java program. If the
    // OutputFormat is a hadoop packaged one, the implementation
    // should use the new API based one under
    // org.apache.hadoop.mapreduce. If it is a custom OutputFormat,
    // it should be implemented using the new API under
    // org.apache.hadoop.mapreduce. The checkOutputSpecs() method
    // of the OutputFormat will be called by pig to check the
    // output location up-front. This method will also be called as
    // part of the Hadoop call sequence when the job is launched. So
    // implementations should ensure that this method can be called
    // multiple times without inconsistent side effects. 
    public OutputFormat getOutputFormat() throws IOException {
        HFileOutputFormat outputFormat = new HFileOutputFormat();
        return outputFormat;
    }

    //
    // setStoreLocation()
    // This method is called by Pig to communicate the store location
    // to the storer. The storer should use this method to communicate
    // the same information to the underlying OutputFormat. This
    // method is called multiple times by pig - implementations should
    // bear in mind that this method is called multiple times and should
    // ensure there are no inconsistent side effects due to the multiple
    // calls.
    public void setStoreLocation(String location, Job job) throws IOException {
        job.getConfiguration().set("mapred.textoutputformat.separator", "");
        FileOutputFormat.setOutputPath(job, new Path(location));
    }

    //
    // prepareToWrite()
    //
    // In the new API, writing of the data is through the OutputFormat provided
    // by the StoreFunc. In prepareToWrite() the RecordWriter associated with
    // the OutputFormat provided by the StoreFunc is passed to the StoreFunc.
    // The RecordWriter can then be used by the implementation in putNext() to
    // write a tuple representing a record of data in a manner expected by the
    // RecordWriter.
    public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) throws IOException {
        this.writer = writer;
    }

    //
    // Here we are going to get the following:
    //
    // (row_key, {(field1),(field2),...})
    //
    // we must iterate through the tuples in the
    // bag and insert them into a TreeSet for
    // sorting. Then we need to iterate through
    // the sorted set and serialize each column
    // 
    //
    @SuppressWarnings("unchecked")
    public void putNext(Tuple t) throws IOException {
        try {
            byte[] rowKey         = Bytes.toBytes(t.get(0).toString()); // use zeroth field as row key
            //byte[] rowKey         = ((DataByteArray)t.get(0)).get();
            DataBag columns       = (DataBag)t.get(1);
            ImmutableBytesWritable hbaseRowKey = new ImmutableBytesWritable(rowKey);
            TreeSet<KeyValue> map = sortedKeyValues(rowKey, columns);
            for (KeyValue kv: map) {
                writer.write(hbaseRowKey, kv);
            }
        } catch (InterruptedException e) {
            throw new IOException("Interrupted");
        } catch (NullPointerException e) {
            System.out.println("@('_')@ Null pointer exception.");
        }
    }

    private TreeSet<KeyValue> sortedKeyValues(byte[] rowKey, DataBag columns) throws IOException {
        TreeSet<KeyValue> map              = new TreeSet<KeyValue>(KeyValue.COMPARATOR);
        long ts                            = System.currentTimeMillis();
        int idx                            = 0;
        Iterator<Tuple> tupleIter = columns.iterator();
        while(tupleIter.hasNext()) {
            byte[] columnName = Bytes.toBytes(columnNames[idx]);
            byte[] value      = Bytes.toBytes((String)tupleIter.next().get(0));                
            if (idx != 0) {
                KeyValue kv = new KeyValue(rowKey, columnFamily, columnName, ts, value);
                map.add(kv.clone());
            }
            idx += 1;
        }
        return map;
    }

    // private byte[] getValue(Object field) throws IOException {
    //     byte[] value = null; 
    //     switch (DataType.findType(field)) {
    //     case DataType.INTEGER: {
    //         value = ((Integer)field).toString().getBytes();
    //         break;
    //     }
    //     case DataType.LONG: {
    //         value = ((Long)field).toString().getBytes();
    //         break;
    //     }
    // 
    //     case DataType.FLOAT: {
    //         value = ((Float)field).toString().getBytes();
    //         break;
    //     }
    // 
    //     case DataType.DOUBLE: {
    //         value = ((Double)field).toString().getBytes();
    //         break;
    //     }
    // 
    //     case DataType.CHARARRAY: {
    //         value = ((String)field).getBytes("UTF-8");
    //         break;
    //     }
    // 
    //     case DataType.BYTEARRAY: {
    //         value = ((DataByteArray)field).get();
    //         break;
    //     }
    // 
    //     default: {
    //         throw new IllegalArgumentException("You fail:" + DataType.findType(field));
    //     }
    //     }
    //     return value;
    // }
}
