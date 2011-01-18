hadoop jar ../build/hbase_bulkloader.jar HbaseGraphTableBulkLoader -Dhbase.table.name=AAtsignsB -Dhbase.column.names=-,-,-,-,tweet_id,created_at,user_b_sn,rel_tw_id -Dhbase.key.indexes=1,3,2 -Dhbase.family.name=base s3://s3hdfs.infinitemonkeys.info/data/sn/tw/fixd/current/a_atsigns_b/part-00000

