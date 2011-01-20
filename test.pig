register 'build/hbase_bulkloader.jar';


--
-- This is the ideal scenario:
-- 
-- 1. We read in data from HDFS and cut out the fields we want.
-- 2. We use a custom partioner, only available in pig >= 0.8,
--    and group the data by the row_key.
-- 3. This data is then sent to HFileStorage. Once in HFileStorage
--    the fields for a given row key are sorted (sorting the strings is not sufficient)
--    and dumped to the HDFS as HFiles for moving into HBase.
-- 4. The $HBASE_HOME/bin/loadtable.rb command is run in the folling way:
--    '$HBASE_HOME/bin/hbase org.jruby.Main $HBASE_HOME/bin/loadtable.rb Jacob /tmp/pig_test/out'
--    This step takes a trivial amount of time to complete.
-- 5. You then must change permissions so hbase can own the table:
--    'hadoop fs -chown -R /hadoop/hbase/Jacob'
--    
-- IDEAL CODE
--
-- twuid      = LOAD '/tmp/streamed/twitter_user_id' AS (rsrc:chararray,uid:chararray,scrat:chararray,sn:chararray,prot:chararray,foll:chararray,friend:chararray,status:chararray,favo:chararray,crat:chararray,sid:chararray,is_full:chararray,health:chararray);
-- cut_fields = FOREACH twuid GENERATE uid, scrat, sn, prot,foll,friend,status,favo,crat,sid,is_full,health;
-- grpd       = GROUP cut_fields BY uid PARTITION BY org.apache.hadoop.hbase.mapreduce.SimpleTotalOrderPartitioner PARALLEL 10;

-- STORE ordrd INTO '/tmp/pig_test/out' USING org.apache.hadoop.hbase.mapreduce.HFileStorage('Jacob', 'pig_test', 'screen_name,protected,followers_count,friends_count,statuses_count,favorites_count,created_at,search_id,is_full,health');
-- 
-- END IDEAL CODE

--
-- Real scenario (Apache release of Pig 0.8 doesn't work with cloudera hadoop cdh3, bummer)
--
-- 1. We read in data from HDFS and cut out the fields we want.
-- 2. We group the data by row_key and don't use a custom partioner
-- 3. We order the grouped data by row_key asc.
--
-- Everything else the same as ideal scenario. Notice that real scenario requires
-- one more reduce
--
-- twuid      = LOAD '/tmp/streamed/twitter_user_id/part-00000' AS (rsrc:chararray,uid:chararray,scrat:chararray,sn:chararray,prot:chararray,foll:chararray,friend:chararray,status:chararray,favo:chararray,crat:chararray,sid:chararray,is_full:chararray,health:chararray);
-- -- cut_fields = FOREACH twuid GENERATE uid, scrat, sn, prot,foll,friend,status,favo,crat,sid,is_full,health;
-- cut_fields = FOREACH twuid GENERATE uid, sn;
-- grpd       = GROUP cut_fields BY uid PARALLEL 1;
-- ordrd      = ORDER grpd BY * ASC;
-- -- 
-- -- DESCRIBE grpd;
-- rmf /tmp/pig_test/out
-- -- STORE grpd INTO '/tmp/pig_test/out' USING org.apache.hadoop.hbase.mapreduce.HFileStorage('Jacob', 'pig_test', 'user_id,screen_name,protected,followers_count,friends_count,statuses_count,favorites_count,created_at,search_id,is_full,health');
-- STORE ordrd INTO '/tmp/pig_test/out' USING org.apache.hadoop.hbase.mapreduce.HFileStorage('Jacob', 'pig_test', 'user_id,screen_name');

twuid      = LOAD '/tmp/streamed/twitter_user_id/part-00000' AS (rsrc:chararray,uid:chararray,scrat:chararray,sn:chararray,prot:chararray,foll:chararray,friend:chararray,status:chararray,favo:chararray,crat:chararray,sid:chararray,is_full:chararray,health:chararray);
cut_fields = FOREACH twuid GENERATE uid, sn;
STORE twuid INTO 'Jacob' USING org.apache.pig.backend.hadoop.hbase.HBaseStorage('my_col_fam:screen_name');
