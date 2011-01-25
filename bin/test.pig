--
-- Load example using pig and HBase TableOutputFormat
--
register 'build/hbase_bulkloader.jar';
register '/usr/lib/hbase/lib/jline-0.9.94.jar';
register '/usr/lib/hbase/lib/guava-r05.jar';
register '/usr/local/share/pig/build/pig-0.8.0-SNAPSHOT-core.jar';
        
data = LOAD '$INPUT' AS (field_1:chararray, field_2:int);
STORE cut_fields INTO '$TABLE' USING com.infochimps.hbase.pig.HBaseStorage('$CF:field_1 $CF:field_2');
