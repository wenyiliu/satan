package com.recommand.study.sql

import java.time.LocalDate

/**
 * @author liuwenyi
 * @date 2021/01/10
 */
object HiveSql {

  val select_all_user_listen: String = "select userId," +
    "musicId," +
    "cast(remaintime as double)," +
    "cast(durationhour as double) " +
    "from hive_test.user_listen "

  /**
   * 查询 mremain 所有数据
   */
  val get_all_remain_data = "select * from mremain"

  /**
   * 查询 mitem 所有数据
   */
  val get_all_mitem_data = "select * from mitem"

  /**
   * 查询 listen 所有数据
   */
  val get_all_user_listen_data = "select * from user_listen limit 10000"

  /**
   * 获取所有用户详情
   */
  val get_all_user_profile = "select * from user_profile"

  /**
   * 展示所有表
   */
  val show_tables = "show tables"

  val music_recall_molecular_sql: String = "select a.musicId as musicTargetId," +
    "       b.musicId as musicSimId," +
    "       sum(cast(a.favoriteRate as double)*cast(b.favoriteRate as double))/2 as molecular " +
    "from user_music_favoriteRate_table as a " +
    "inner join user_music_favoriteRate_table as b " +
    "on a.userId = b.userId " +
    "and a.musicId != b.musicId " +
    "group by a.musicId,b.musicId"

  val music_recall_denominator_sql: String = "select musicId," +
    "sqrt(sum(cast(favoriteRate as double)*cast(favoriteRate as double))) as denominator " +
    "from user_music_favoriteRate_table " +
    "group by musicId"

  val music_recall_sim_sql: String = "select a.musicTargetId," +
    "       a.musicSimId," +
    "       a.molecular/(b.denominator * c.denominator) as sim " +
    "from music_2_music_molecular_table as a " +
    "inner join music_2_music_denominator_table as b on a.musicTargetId = b.musicId " +
    "inner join music_2_music_denominator_table as c on a.musicSimId = c.musicId"

  val music_recall_sim_top30_sql: String = "select * " +
    "                              from (select *," +
    "                                           row_number() over(partition by musicTargetId order by sim desc) as rank " +
    "                                    from music_2_music_sim_table) as a " +
    "                             where a.rank <=30"

  val user_favorite_rate_top20_sql: String = "select * " +
    "         from (select *," +
    "               row_number() over(partition by userId order by favoriteRate desc) as rank " +
    "               from user_music_favoriteRate_table" +
    "               ) as a " +
    "         where a.rank <= 20"

  val music_sim_sql: String = "select a.musicTargetId as musicId," +
    "                       sum((cast(b.favoriteRate as double)*cast(a.sim as double)))/sum(cast(a.sim as double)) as sim " +
    "from music_2_music_sim_top30_table as a " +
    "inner join user_music_favorite_top20_table as b on a.musicSimId = b.musicId " +
    " where 1=1 group by a.musicTargetId "

  val music_all_sql = "select distinct musicId from user_music_favoriteRate_table"

  def save_music_recall_table_sql(tableName: String): String = s"insert into $tableName partition(date='${LocalDate.now().toString}') " +
    s"select user_id," +
    s"       music_id," +
    s"       score " +
    s"from music_recall_tmp_table"
}
