register 'build/hbase_bulkloader.jar';
twuid      = LOAD '/tmp/streamed/twitter_user_id/part-00001' AS (rsrc:chararray,uid:chararray,scrat:chararray,sn:chararray,prot:chararray,folay,foll:chararray,friend:chararray,status:chararray,favo:chararray,crat:chararray,sid:chararray,is_full:chararray,health:chararray);
cut_fields = FOREACH twuid GENERATE uid, sn;
filtered   = FILTER cut_fields BY (uid IS NOT NULL) AND (sn IS NOT NULL);
ordrd      = ORDER cut_fields BY * ASC;
-- -- needs to write to hdfs file, idiot

rmf /tmp/pig_test/out;
STORE ordrd INTO '/tmp/pig_test/out' USING org.apache.hadoop.hbase.mapreduce.HFileStorage('Jacob', 'pig_test', 'screen_name');
