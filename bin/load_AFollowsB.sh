hadoop jar ../build/hbase_bulkloader.jar HbaseGraphBulkLoader -Dhbase.table.name=AFollowsB -Dhbase.family.name=base s3://s3hdfs.infinitemonkeys.info/data/sn/tw/fixd/current/a_follows_b/part-00000

