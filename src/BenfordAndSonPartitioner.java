package org.apache.hadoop.hbase.mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Partitioner;

public class BenfordAndSonPartitioner<VALUE> extends Partitioner<ImmutableBytesWritable, VALUE> implements Configurable {
  private final static Log LOG = LogFactory.getLog(BenfordAndSonPartitioner.class);
  private Configuration c;

  @Override
  public int getPartition(final ImmutableBytesWritable key, final VALUE value, final int reduces) {
    if (reduces == 1) return 0;
    if (key.getSize() < 4) return 0;

    Integer segment = (Integer)BenfordAndSon.segments.get(key);
    int part    = (segment*reduces)/BenfordAndSon.segments.size();
    LOG.info("key = "+key.toString()+", segment = "+segment+", part = "+part);
    return part;
  }

  @Override
  public Configuration getConf() {
    return this.c;
  }

  @Override
  public void setConf(Configuration conf) {
    this.c = conf;
  }
}
