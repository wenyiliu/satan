package com.recommand.study.util

import com.recommand.study.commonenum.RecallCFEnum
import com.recommand.study.data.HiveDataLoader
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author liuwenyi
 * @date 2021/01/13
 */
object RecallCFUtils {

  /**
   * 获取用户对音乐的喜爱程度
   *
   * @param spark SparkSession
   * @return DataFrame
   */
  def getUserMusicFavoriteRate(spark: SparkSession): DataFrame = {
    import spark.implicits._
    // 用户收听音乐详情
    val userListenDetailDS = HiveDataLoader.getUserListenData(spark)

    // 用户每首歌听的时长
    val userMusicListenSum = userListenDetailDS.rdd
      .map(userListenDetail => (userListenDetail.userId + "_" + userListenDetail.musicId, userListenDetail))
      .groupByKey()
      .mapValues(iter => iter.map(userListen => userListen.remaintime.toDouble).sum)
      .map(value => {
        val valuesArr = value._1.split("_")
        (valuesArr(0), valuesArr(1), value._2)
      })
      .toDF(RecallCFEnum.userId.toString, RecallCFEnum.musicId.toString, "lesionMusicTimeSum")

    // 用户听歌总时长
    val userListenSum = userListenDetailDS.rdd
      .map(userListenDetail => (userListenDetail.userId, userListenDetail))
      .groupByKey()
      .mapValues(iter => iter.map(userListen => userListen.remaintime.toDouble).sum)
      .toDF(RecallCFEnum.userId.toString, "lesionTimeSum")

    // 用户对每首歌的喜爱程度，【某首歌的听歌时长/用户听歌总时长】
    userMusicListenSum.join(userListenSum, RecallCFEnum.userId.toString)
      .selectExpr(RecallCFEnum.userId.toString, RecallCFEnum.musicId.toString, "lesionMusicTimeSum/lesionTimeSum as favoriteRate")
  }

  def getFilterCondition(column: String): String = {
    s"$column <> ${CommonUtils.concatCopy(column)}"
  }
}
