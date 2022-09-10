package com.recommand.study.data

import com.recommand.study.bean.UserListenDetailBean
import com.recommand.study.sql.HiveSql
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @author liuwenyi
 * @date 2021/01/11
 */
object HiveDataLoader {

  def getUserRemainData(spark: SparkSession): DataFrame = {
    spark.sql(HiveSql.get_all_remain_data)
  }

  def getItemData(spark: SparkSession): DataFrame = {
    spark.sql(HiveSql.get_all_mitem_data)
  }

  def getUserProfile(spark: SparkSession): DataFrame = {
    spark.sql(HiveSql.get_all_user_profile)
  }

  def getUserListenData(spark: SparkSession): Dataset[UserListenDetailBean] = {
    import spark.implicits._
    spark.sql(HiveSql.get_all_user_listen_data).as[UserListenDetailBean]
  }

  def tableExists(spark: SparkSession, tableName: String): Boolean = {
    val tables = spark.sql(HiveSql.show_tables)
    tables.rdd.println
    tables.rdd.map(item => item.get(1).toString).filter(name => name.equals(tableName))
      .count() > 0
  }
}
