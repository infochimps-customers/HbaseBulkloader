package com.infochimps.hbase.pig;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

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
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.pig.LoadCaster;
import org.apache.pig.StoreFunc;
import org.apache.pig.LoadPushDown;
import org.apache.pig.LoadStoreCaster;
import org.apache.pig.ResourceSchema;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.StoreFuncInterface;
import org.apache.pig.backend.hadoop.hbase.HBaseBinaryConverter;
import org.apache.pig.builtin.Utf8StorageConverter;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.data.BagFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.util.Utils;


import com.google.common.collect.Lists;

/**
 * Different from default HBaseStorage in that a field in the data is used as the column family name
 * and another field is used as the column name.
 *
 */
public class DynamicFamilyStorage extends StoreFunc implements StoreFuncInterface {
    
    private static final Log LOG = LogFactory.getLog(DynamicFamilyStorage.class);

    private final static String STRING_CASTER   = "UTF8StorageConverter";
    private final static String BYTE_CASTER     = "HBaseBinaryConverter";
    private final static String CASTER_PROPERTY = "pig.hbase.caster";
    
    private List<byte[]> columnList_ = Lists.newArrayList();
    private HTable m_table;

    private Configuration m_conf;
    private RecordReader reader;
    private RecordWriter writer;
    private Scan scan;

    private LoadCaster caster_;
    private ResourceSchema schema_;

    public DynamicFamilyStorage() throws IOException {
        m_conf  = HBaseConfiguration.create();
        caster_ = new Utf8StorageConverter();
    }

    @Override
    public OutputFormat getOutputFormat() throws IOException {
        TableOutputFormat outputFormat = new TableOutputFormat();
        return outputFormat;
    }

    @Override
    public void checkSchema(ResourceSchema s) throws IOException {
        if (! (caster_ instanceof LoadStoreCaster)) {
            LOG.error("Caster must implement LoadStoreCaster for writing to HBase.");
            throw new IOException("Bad Caster " + caster_.getClass());
        }
        schema_ = s;
    }

    @Override
    public void prepareToWrite(@SuppressWarnings("rawtypes") RecordWriter writer) throws IOException {
        this.writer = writer;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void putNext(Tuple t) throws IOException {
        byte[] rowKey = objToBytes(t.get(0), DataType.findType(t.get(0))); // Convert row key to byte[]
        
        if(rowKey != null && t.size() == 4) {
            long ts = System.currentTimeMillis();
            Put put = new Put(rowKey);
            put.setWriteToWAL(false);
            
            byte[] family  = objToBytes(t.get(1), DataType.findType(t.get(1)));
            byte[] colName = objToBytes(t.get(2), DataType.findType(t.get(2)));
            byte[] colVal  = objToBytes(t.get(3), DataType.findType(t.get(3)));             
            if (colVal!=null) {
                put.add(family, colName, ts, colVal);
            }
            try {
                if (!put.isEmpty()) { // Don't try to write a row with 0 columns
                    writer.write(null, put);
                }
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    private byte[] objToBytes(Object o, byte type) throws IOException {
        LoadStoreCaster caster = (LoadStoreCaster) caster_;
        switch (type) {
        case DataType.BYTEARRAY: return ((DataByteArray) o).get();
        case DataType.BAG: return caster.toBytes((DataBag) o);
        case DataType.CHARARRAY: return caster.toBytes((String) o);
        case DataType.DOUBLE: return caster.toBytes((Double) o);
        case DataType.FLOAT: return caster.toBytes((Float) o);
        case DataType.INTEGER: return caster.toBytes((Integer) o);
        case DataType.LONG: return caster.toBytes((Long) o);
        
        // The type conversion here is unchecked. 
        // Relying on DataType.findType to do the right thing.
        case DataType.MAP: return caster.toBytes((Map<String, Object>) o);
        
        case DataType.NULL: return null;
        case DataType.TUPLE: return caster.toBytes((Tuple) o);
        case DataType.ERROR: throw new IOException("Unable to determine type of " + o.getClass());
        default: throw new IOException("Unable to find a converter for tuple field " + o);
        }
    }

    @Override
    public String relToAbsPathForStoreLocation(String location, Path curDir)
    throws IOException {
        return location;
    }

    @Override
    public void setStoreFuncUDFContextSignature(String signature) { }

    @Override
    public void setStoreLocation(String location, Job job) throws IOException {
        if (location.startsWith("hbase://")){
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, location.substring(8));
        }else{
            job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, location);
        }
    }

    @Override
    public void cleanupOnFailure(String location, Job job) throws IOException {

    }
}
