create table user_movies(user_id INT,movie_info STRING);
create table user_movies(id int,info string)
stored by 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    with serdeproperties("hbase.columns.mapping"=":key,st1:info")
    tblproperties("hbase.table.name"="user_movies","hbase.mapred.output.outputtable" = "user_movies");