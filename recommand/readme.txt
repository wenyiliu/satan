-- 创建 user_recall 分区表
CREATE TABLE if not exists user_recall (
user_id string,
music_id string,
score String
)
PARTITIONED BY (`date` string)
row format delimited fields terminated by ','
stored as ORC;

-- 创建 music_recall 分区表
CREATE TABLE if not exists music_recall (
user_id string,
music_id string,
score String
)
PARTITIONED BY (`date` string)
row format delimited fields terminated by ','
stored as ORC;