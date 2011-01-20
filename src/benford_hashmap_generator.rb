#!/usr/bin/env ruby
require 'configliere'; Settings.use :commandline

#
# This generates a java file containing a hashmap for fairly partitioning numeric strings
#
#   http://en.wikipedia.org/wiki/Benford's_law
#
# The idea is to pull off the first few characters (the "prefix") and map it to
# a more-or-less fairly distributed
#
# Prefixes are strings of length prefix_chars that contain only ascii numbers.
#
# The typical use case for this partitioner is a keyspace with millions or more
# possible values. For concreteness, let's say you generate the hashmap with
# the default, prefix_chars=4.
#
# We have to deal with the following cases:
#
# * Short Portion: the key is a number less than 1000 (so, a string of fewer
#   than 4 characters). We know what the distribution of these is: one in 1000
#   within the short portion (1001 if you include the empty string "").
#
# * Main Portion: keys from 1000 to 9999. Prefixes are distributed within the
#   main portion according to Benford's law:
#
#     P(n) = log10( 1 + 1.0 / n )
#
# In the case where there are 10,000 keys, the short portion is 10% of the main
# portion. For 100,000 keys, it is 1%; for 100k it's 0.1%, and so forth.
#
# My point is, we're only going to calculate the distribution of the main
# portion. In the actual partitioner, we're just going to lump strings from the
# short portion into segment zero. This mildly skews the distribution if your
# keyspace is small, but if your keyspace is small why the fuck are you using
# this partitioner?
#

Settings.define :prefix_chars, :default =>   4, :type => Integer, :description => "How many characters you'll snip from each string. The hashmap will be about 10**prefix_chars in size (So with prefix_chars=4, have about 10,000 rows)"
Settings.define :segments,     :default => 144, :type => Integer, :description => "How many output segments to use; make this be well less than 10**prefix_chars."
Settings.resolve!
THIS_DIR = File.dirname(__FILE__)
GENERATED_JAVA_FILE = File.join(THIS_DIR, "BenfordAndSon.java")

# ===========================================================================
#
# Assemble a sorted list of prefixes, from 1000 to 9999 (in the case of
# prefix_chars == 4), or analogously
#

prefixes = (10**(Settings.prefix_chars-1) .. (10**Settings.prefix_chars - 1))

# ===========================================================================
#
# Calculate the distribution of prefixes to segments
#

def prefixes_to_benfords_law_segments prefixes
  benford_map = {}
  tot_prob = 0
  prefixes.each do |prefix|
    next if prefix == 0
    prob = Math.log10( 1 + (1.0 / prefix.to_f) )
    tot_prob += prob
    segment = (Settings.segments * tot_prob).to_i
    # p [prefix, prob, tot_prob, segment]
    benford_map[prefix] = segment
  end
  benford_map
end

# ===========================================================================
#
# Stuff the prefix => segment map into a java hashmap
#

# ---------------------------------------------------------------------------
# start template
HASHMAP_TEMPLATE = %Q{
/*
 * This file is auto-generated!! Don\'t overwrite it, you big dummy!
 * See benford_hashmap_generator.rb for more info
 *
 */

package org.apache.hadoop.hbase.mapreduce;

import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

class BenfordAndSon {
  public final static Map segments = new HashMap<byte[], Integer>() {
    {
%s
    }
  };
}
}
# end template
# ---------------------------------------------------------------------------

def java_hashmap_entry key, val
  %Q{      put(new Bytes.toBytes("#{key}"), #{val.to_i});}
end

$stderr.puts "Created hash map from #{prefixes.to_a.length} prefixes (all numeric strings of length #{Settings.prefix_chars} or less) to #{Settings.segments} segments; storing in #{GENERATED_JAVA_FILE}"
prefix_to_segment = prefixes_to_benfords_law_segments(prefixes)
File.open(GENERATED_JAVA_FILE, "w") do |generated_java_file|
  hashmap_entries = prefix_to_segment.map{|prefix, segment| java_hashmap_entry(prefix, segment) }.join("\n")
  generated_java_file.puts( HASHMAP_TEMPLATE % hashmap_entries )
end


