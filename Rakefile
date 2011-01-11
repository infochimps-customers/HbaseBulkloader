#!/usr/bin/env ruby

require 'rubygems'
require 'wukong'
require 'configliere' ; Configliere.use(:commandline, :env_var, :define)

WORK_DIR=File.expand_path(File.dirname(__FILE__))
Settings.define :src,         :default => "#{WORK_DIR}/src",                :description => "Java source dir"
Settings.define :target,      :default => "#{WORK_DIR}/build",              :description => "Build target, this is where compiled classes live"
Settings.define :main_class,  :default => "HbaseBulkloader",                :description => "Main java class to run"
Settings.define :hadoop_home, :default => "/usr/lib/hadoop",                :description => "Path to hadoop installation",       :env_var => "HADOOP_HOME"
Settings.define :hbase_home,  :default => "/usr/lib/hbase",                 :description => "Path to hbase installation",        :env_var => "HBASE_HOME"
Settings.resolve!
options = Settings.dup

#
# Returns full classpath
#
def classpath options
  cp = ["."]
  Dir[
    "#{options.hadoop_home}/hadoop*.jar",
    "#{options.hadoop_home}/lib/*.jar",
    "#{options.hbase_home}/hbase*.jar",
    "#{options.hbase_home}/lib/*.jar",
  ].each{|jar| cp << jar}
  cp.join(':')
end

def srcs options
  sources = Dir[
    "#{options.src}/*.java",
  ].inject([]){|sources, src| sources << src; sources}
  sources.join(' ')
end

#
# FIXME: Needs to be idempotent ...
#
task :compile do
  puts "Compiling #{options.src} ..."
  snakeized = options.main_class.underscore
  mkdir_p File.join(options.target, snakeized)
  system "javac -cp #{classpath(options)} -d #{options.target}/#{snakeized} #{srcs(options)}"
  system "jar -cvf  #{options.target}/#{snakeized}.jar -C #{options.target}/#{snakeized} . "
end

task :default => [:compile]
