package com.infochimps.hadoop;

import java.util.Random;
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
  private Random randgen = new Random();

  @Override
  public int getPartition(final ImmutableBytesWritable key, final VALUE value, final int reduces) {
    if (reduces == 1) return 0;
    if (key.getSize() < 4) return 0;

    // 'slice' the prefix off the key and look up segment in hashmap
    String keyString = Bytes.toString(key.get());
    String prefix    = keyString.substring(0,3);

    Float bin = (Float)TwitterUserIdDistribution.distribution.get(prefix); // Yikes, FIXME!
    // Float bin = (Float)BenfordAndSon.distribution.get(prefix);
    int part = 0;
    if(bin!=null) {
        part  = (int)(bin.floatValue()*reduces);
        if (randgen.nextDouble() < 0.001) {LOG.info("key = "+key.toString()+", bin = "+bin+", prefix = "+prefix+", part = "+part);};
    } else {
        bin = 0.0f;
        part  = (int)(bin.floatValue()*reduces);
        if (randgen.nextDouble() < 0.001) {LOG.info("key = "+key.toString()+", bin = "+bin+", prefix = "+prefix+", part = "+part);};
    }
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
