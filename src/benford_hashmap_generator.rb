#!/usr/bin/env ruby
require 'configliere'; Settings.use :commandline

#
# This generates a java file containing a hashmap for fairly partitioning numeric strings
#
#   http://en.wikipedia.org/wiki/Benford's_law
#
#

Settings.define :prefix_chars, :default =>   4, :type => Integer, :description => "How many characters you'll snip from each string. The hashmap will be about 10**prefix_chars in size (So with prefix_chars=4, have about 10,000 rows)"
Settings.define :segments,     :default => 144, :type => Integer, :description => "How many output segments to use; make this be well less than 10**prefix_chars."
Settings.resolve!
THIS_DIR = File.dirname(__FILE__)
GENERATED_JAVA_FILE = File.join(THIS_DIR, "BenfordAndSon.java")

# ===========================================================================
#
# Assemble all the prefixes:
# Strings of length 0 to prefix_chars that contain only ascii numbers.
#

PREFIX_CHARS = ["", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]

def expand_prefixes prefixes
  PREFIX_CHARS.map{|char| prefixes.map{|prefix| char + prefix }}.flatten.uniq
end

prefixes = [""]
Settings.prefix_chars.times do
  prefixes = expand_prefixes(prefixes)
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

class BenfordAndSon {
  public final static Map segments = new HashMap<ImmutableBytesWriteable, Integer>() {
    {
%s
    }
  };
}
}
# end template
# ---------------------------------------------------------------------------

def java_hashmap_entry key, val
  %Q{      put(new ImmutableBytesWriteable(Bytes.toBytes("#{key}")), #{val.to_i});}
end

$stderr.puts "Created hash map from #{prefixes.length} prefixes (all numeric strings of length #{Settings.prefix_chars} or less) to #{Settings.segments} segments; storing in #{GENERATED_JAVA_FILE}"
File.open(GENERATED_JAVA_FILE, "w") do |generated_java_file|
  generated_java_file.puts( HASHMAP_TEMPLATE % prefixes.map{|prefix| java_hashmap_entry(prefix, prefix) }.join("\n") )
end

