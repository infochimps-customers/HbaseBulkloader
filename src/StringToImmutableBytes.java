package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.util.WrappedIOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public class StringToImmutableBytes extends EvalFunc<ImmutableBytesWritable>
{
    public ImmutableBytesWritable exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try {
            byte[] key = Bytes.toBytes((String)input.get(0));
            ImmutableBytesWritable immutableKey = new ImmutableBytesWritable(key);
            return immutableKey;
        } catch(Exception e) {
            throw WrappedIOException.wrap("Caught exception processing input row ", e);
        }
    }
}
