hadoop jar ../build/hbase_bulkloader.jar HbaseTableBulkLoader -Dhbase.table.name=TwitterUser -Dhbase.column.names=-,user_id,scraped_at,screen_name,protected,followers_count,friends_count,statuses_count,favourites_count,created_at,listed_count -Dhbase.key.index=1 -Dhbase.family.name=base s3://s3hdfs.infinitemonkeys.info/data/sn/tw/fixd/current/twitter_user_id/

