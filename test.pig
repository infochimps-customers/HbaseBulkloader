register build/hbase_bulkloader.jar
register /usr/lib/hbase/lib/jline-0.9.94.jar
register /usr/lib/hbase/lib/guava-r05.jar

%default TABLE 'a_rel_b_20110128'

-- we would like to get back a tuple when given colfam:colname and a bag when given only colfam:
data = LOAD '$TABLE' USING com.infochimps.hbase.pig.HBaseStorage('follow:ab follow:ba reply: retweet: mention: ', '-limit 1000 -loadKey');
DUMP data;
