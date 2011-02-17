. set_hadoop_classpath.sh

hadoop jar ../build/hbase_bulkloader.jar com.infochimps.hbase.HbaseTableBulkLoader -Dhbase.table.name=soc_net_tw_twitter_user -Dhbase.column.names=-,user_id,scraped_at,screen_name,protected,followers_count,friends_count,statuses_count,favourites_count,created_at,listed_count -Dhbase.key.index=1 -Dhbase.family.name=base s3n://cassandra-test-data/twitter_user

hadoop jar ../build/hbase_bulkloader.jar com.infochimps.hbase.HbaseAFBLoader -Dhbase.table.name=soc_net_tw_a_rel_b s3n://cassandra-test-data/a_follows_b/

hadoop jar ../build/hbase_bulkloader.jar com.infochimps.hbase.HbaseAAtsignsBLoader -Dhbase.table.name=soc_net_tw_a_rel_b s3n://cassandra-test-data/a_atsigns_b
