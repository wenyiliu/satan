package com.recommand.study.recall

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author liuwenyi
 * @date 2021/01/18
 */
trait RecallCFTrait {

  /**
   * 获取过滤条件
   *
   * @param column 列名
   * @return 过滤条件
   */
  def getFilterCondition(column: String): String

  /**
   * 获取用户对音乐的喜爱程度
   *
   * @param spark SparkSession
   * @return DataFrame
   */
  def getUserMusicFavoriteRate(spark: SparkSession): DataFrame
}
