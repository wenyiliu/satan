package com.recommand.study.sql

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
  val get_all_user_listen_data = "select * from user_listen limit 3000"

  /**
   * 获取所有用户详情
   */
  val get_all_user_profile = "select * from user_profile"

  /**
   * 展示所有表
   */
  val show_tables = "show tables"
}
