#!/usr/bin/env ruby

require 'rubygems'
require 'configliere' ; Configliere.use(:commandline, :env_var, :define)

WORK_DIR=File.expand_path(File.dirname(__FILE__))
Settings.define :src,         :default => "#{WORK_DIR}/src",                :description => "Java source dir"
Settings.define :target,      :default => "#{WORK_DIR}/build",              :description => "Build target, this is where compiled classes live"
Settings.define :jar_name,    :default => "hbase_bulkloader",               :description => "Name of output jar"
Settings.define :hadoop_home, :default => "/usr/lib/hadoop",                :description => "Path to hadoop installation",       :env_var => "HADOOP_HOME"
Settings.define :pig_home,    :default => "/usr/local/share/pig",           :description => "Path to pig installation",          :env_var => "PIG_HOME"
Settings.define :hbase_home,  :default => "/usr/lib/hbase",                 :description => "Path to hbase installation",        :env_var => "HBASE_HOME"
Settings.resolve!
options = Settings.dup

#
# Returns full classpath
#
def classpath options, delim=":"
  cp = ["."]
  Dir[
    "#{options.hadoop_home}/hadoop*.jar",
    "#{options.hadoop_home}/lib/*.jar",
    "#{options.hbase_home}/hbase*.jar",
    "#{options.hbase_home}/lib/*.jar",
    "#{options.pig_home}/pig*.jar",
    "#{options.pig_home}/build/pig*.jar",
    "#{options.pig_home}/lib/*.jar",
  ].each{|jar| cp << jar}
  cp.join(delim)
end

def srcs options
  sources = Dir[
    "#{options.src}/**/*.java",
  ].inject([]){|sources, src| sources << src; sources}
  sources.join(' ')
end

jar_dir = File.join(options.target, options.jar_name)
directory jar_dir

task :compile => jar_dir do
  sh "$JAVA_HOME/../bin/javac -cp #{classpath(options)} -d #{jar_dir} #{srcs(options)}"
end

task :jar => :compile do
  sh "jar -cvf  #{jar_dir}.jar -C #{jar_dir} . "
end

task :default => [:jar]
