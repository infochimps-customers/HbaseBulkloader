#!/usr/bin/env ruby
require 'configliere'; Settings.use :commandline

#
# This generates a java file containing a hashmap for fairly partitioning numeric strings
#
#   http://en.wikipedia.org/wiki/Benford's_law
#
# The idea is to pull off the first few characters (the "prefix") and map it to
# more-or-less fairly distributed bis.  An evenly distributed set of numbers
# will have a Benford-distributed set of numeric strings, and will fairly evenly
# populate each partition.
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
Settings.define :sampled_distribution, :default => nil, :description => "If left empty, gives a theoretical Benford's law distribution. If a filename is specified, it is loaded as a TSV mapping numeric strings to counts, and that distribution is used to generate the hashmap"
Settings.resolve!
THIS_DIR = File.dirname(__FILE__)
GENERATED_JAVA_FILE = File.join(THIS_DIR, "BenfordAndSon.java")


class BenfordDistributionGenerator

  # ===========================================================================
  #
  # Assemble a sorted list of prefixes, from 1000 to 9999 (in the case of
  # prefix_chars == 4), or analogously
  #
  def prefixes
    @prefixes ||= (10**(Settings.prefix_chars-1) .. (10**Settings.prefix_chars - 1))
  end

  # ===========================================================================
  #
  # Calculate the distribution of prefixes from the theoretical Benford's law
  # distribution.
  #
  def distribution_from_benfords_law!
    @distribution = {}
    tot_prob = 0
    prefixes.each do |prefix|
      next if prefix == 0
      @distribution[prefix] = tot_prob
      prob = Math.log10( 1 + (1.0 / prefix.to_f) )
      tot_prob += prob
      # p [prefix, prob, tot_prob]
    end
    @distribution
  end

  # ===========================================================================
  #
  # Calculate the distribution of prefixes from an actual sampled distribution
  #

  def sampled_distribution_file
    @sampled_distribution_file ||= File.open(Settings.sampled_distribution)
  end

  def counts_from_sampled
    prefix_counts = Hash.new{|h,k| h[k] = 0 }
    tot_count     = 0
    sampled_distribution_file.each do |line|
      str, count = line.chomp.split("\t")
      prefix = str[0 .. (Settings.prefix_chars-1)]
      prefix_counts[prefix] += count.to_f
      tot_count             += count.to_f
    end
    [prefix_counts, tot_count]
  end

  def cdf_from_sampled_distribution
    prefix_counts, tot_count = counts_from_sampled
    pdf = {}
    cdf = {}
    running_total = 0
    prefix_counts.each do |prefix, count|
      pdf[prefix]    = (count / tot_count)
      running_total += pdf[prefix]
      cdf[prefix]    = running_total
    end
    cdf
  end

  def distribution_from_sampled_file!
    @distribution = cdf_from_sampled_distribution
  end

  def get_distribution
    if Settings.sampled_distribution.blank?
      distribution_from_benfords_law!
    else
      distribution_from_sampled_file!
    end
    @distribution
  end
end


# ===========================================================================
#
# Stuff the prefix => distribution map into a java hashmap
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
  public final static Map distribution = new HashMap<String, Float>() {
    {
%s
    }
  };
}
}
# end template
# ---------------------------------------------------------------------------

def java_hashmap_entry key, val
  %Q{      put("#{key}", #{"%9.7ff"%val});}
end

dist = BenfordDistributionGenerator.new
$stderr.puts "Created hash map from #{dist.prefixes.to_a.length} prefixes (all numeric strings of length #{Settings.prefix_chars}) and storing it in #{GENERATED_JAVA_FILE}"
File.open(GENERATED_JAVA_FILE, "w") do |generated_java_file|
  hashmap_entries = dist.get_distribution.map{|prefix, bin| java_hashmap_entry(prefix, bin) }.join("\n")
  generated_java_file.puts( HASHMAP_TEMPLATE % hashmap_entries )
end

# p BenfordDistributionGenerator.new.cdf_from_sampled_distribution

