register 'build/hbase_bulkloader.jar';
register '/usr/lib/hbase/lib/jline-0.9.94.jar';
register '/usr/lib/hbase/lib/guava-r05.jar';
register '/usr/local/share/pig/build/pig-0.8.0-SNAPSHOT-core.jar';
        
twuid      = LOAD '/tmp/streamed/twitter_user_id/part-00000' AS (rsrc:chararray,uid:chararray,scrat:chararray,sn:chararray,prot:chararray,foll:chararray,friend:chararray,status:chararray,favo:chararray,crat:chararray,sid:chararray,is_full:chararray,health:chararray);
cut_fields = FOREACH twuid GENERATE uid, scrat, sn, prot, foll, friend, status, favo, crat, sid, is_full, health;
STORE cut_fields INTO 'Jacob' USING
        com.infochimps.hbase.pig.FooStorage(
        'my_col_fam:scraped_at my_col_fam:screen_name my_col_fam:protected my_col_fam:followers_count my_col_fam:friend_count my_col_fam:statuses_count my_col_fam:favourites_count my_col_fam:created_at my_col_fam:search_id my_col_fam:is_full my_col_fam:health'
        );
