hadoop jar build/hbase_bulkloader.jar com.infochimps.hbase.HbaseAFollowsBLoader -Dmapred.reduce.tasks=360 -libjars /usr/lib/hbase/lib/guava-r05.jar /tmp/streamed/a_follows_b /tmp/hfiles/a_follows_b
