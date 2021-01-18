package com.recommand.study.recall

import breeze.numerics.{pow, sqrt}
import com.recommand.study.commonenum.RecallCFEnum
import com.recommand.study.data.HiveDataLoader
import com.recommand.study.util.{CommonUtils, FunctionUDFUtils}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{DataFrame, SparkSession}

import java.time.LocalDate

/**
 * @author liuwenyi
 * @date 2021/01/11
 */
class RecallUserCF(spark: SparkSession) extends BaseRecallCF {

  import spark.implicits._

  // 是否聚类，默认不使用聚类
  private var isCluster = false

  private var tableName = ""

  def this(spark: SparkSession, isCluster: Boolean) {
    this(spark)
    this.isCluster = isCluster
  }

  def this(spark: SparkSession, isCluster: Boolean, tableName: String) {
    this(spark)
    this.isCluster = isCluster
    this.tableName = tableName
  }

  def userItemRecall(): Unit = {
    val userItemRecall = getUserItemRecall
    if (tableName == "" || tableName == null) {
      userItemRecall.show()
    } else if (HiveDataLoader.tableExists(spark, tableName)) {
      saveDataFrame2Hive(userItemRecall, tableName)
    } else {
      throw new IllegalArgumentException("结果异常")
    }
  }

  private def getUserItemRecall: DataFrame = {
    val userMusicFavoriteRate = getUserMusicFavoriteRate(spark)
    userMusicFavoriteRate.cache()

    val userRateMolecularDF = getUserRateMolecularDF(userMusicFavoriteRate)
    //    userRateMolecularDF.show(100)

    // 计算分母
    val userRateDenominator = userMusicFavoriteRate.rdd
      .map(x => (x(0).toString, x(2).toString))
      .groupByKey()
      .mapValues(iter => sqrt(iter.map(rate => pow(rate.toDouble, 2)).sum))
      .toDF(RecallCFEnum.userId.toString, RecallCFEnum.denominator.toString)

    // copy 分母 df
    val userRateDenominatorCopy = userRateDenominator.selectExpr("userId as userSimId", "denominator as denominatorSim")

    // 计算两两用户的相似度 目标用户，相似用户，相似度
    val dfSimDf = userRateMolecularDF.join(userRateDenominator, "userId")
      .join(userRateDenominatorCopy, userRateMolecularDF("userIdCopy") === userRateDenominatorCopy("userSimId"))
      .selectExpr("userId as userTargetId", "userSimId as userSimId", "molecular/(denominator * denominatorSim) as sim")

    // 用户听过音乐的列表
    val userMusicArr = userMusicFavoriteRate.rdd
      .map(x => (x(0).toString, x(1).toString + "_" + x(2).toString))
      .groupByKey()
      .mapValues(x => x.toArray)
      .toDF(RecallCFEnum.userId.toString, "musicArr")

    val userMusicArrCopy = CommonUtils.copyDF(userMusicArr)
    userMusicArrCopy.show(100)
    // 目标用户，目标用户听过的音乐列表，相似用户，相似用户听过的音乐列表
    val user2UserMusicArrDF = dfSimDf.join(userMusicArr, dfSimDf("userTargetId") === userMusicArr(RecallCFEnum.userId.toString))
      .join(userMusicArrCopy, dfSimDf("userSimId") === userMusicArrCopy("userIdCopy"))
      .selectExpr("userTargetId", "userSimId", "musicArr as musicTargetArr", "musicArrCopy as musicSimArr", "sim")
      .withColumn("unListen", FunctionUDFUtils.filterUnListenUdf(col("musicTargetArr"), col("musicSimArr")))

    val unListenDF = user2UserMusicArrDF.withColumn("musicPro", FunctionUDFUtils.simRatingUDF(col("sim"), col("unListen")))
      .selectExpr("userTargetId as userId", "musicPro")
    val itemScoreDF = unListenDF.select(unListenDF("userId"),
      explode(unListenDF("musicPro"))).toDF("userId", "musicPro")
      .selectExpr("userId as user_id ", "split(musicPro,'_')[0] as music_id",
        "split(musicPro,'_')[1] as score")
    //    itemScoreDF.show(10)
    userMusicFavoriteRate.unpersist()
    itemScoreDF
  }

  private def getUserRateMolecularDF(userMusicFavoriteRate: DataFrame): DataFrame = {
    var tmpMusicRelatedDF: DataFrame = null
    if (isCluster) {
      // 使用聚类，获取用户详细信息
      val userProfileDF = HiveDataLoader.getUserProfile(spark)

      // 用户对音乐的喜爱程度 df，与用户详细信息做关联
      val userMusicFavoriteRateProfile = userMusicFavoriteRate.join(userProfileDF, RecallCFEnum.userId.toString)
        .selectExpr(RecallCFEnum.userId.toString, RecallCFEnum.musicId.toString, RecallCFEnum.gender.toString,
          RecallCFEnum.age.toString, RecallCFEnum.salary.toString, RecallCFEnum.favoriteRate.toString)

      // copy 一个带有用户详细信息的 df，后面做关联
      val userMusicFavoriteRateProfileCopy = CommonUtils.copyDF(userMusicFavoriteRateProfile)

      // 关联需要的 on 条件
      val onColumnArr = Array(RecallCFEnum.musicId.toString, RecallCFEnum.gender.toString, RecallCFEnum.age.toString,
        RecallCFEnum.salary.toString)

      // 调用工具类做关联，降低处理的数据量
      tmpMusicRelatedDF = CommonUtils.join(userMusicFavoriteRateProfile, userMusicFavoriteRateProfileCopy, onColumnArr)
        .filter(getFilterCondition(RecallCFEnum.userId.toString))
    } else {
      // 不使用聚类，copy 一个用户喜爱程度
      val userMusicFavoriteRateCopy = CommonUtils.copyDF(userMusicFavoriteRate)
      tmpMusicRelatedDF = CommonUtils.join(userMusicFavoriteRate, userMusicFavoriteRateCopy, Array(RecallCFEnum.musicId.toString))
        .filter(getFilterCondition(RecallCFEnum.userId.toString))
    }
    //    tmpMusicRelatedDF.show(100)
    // 计算 key = userID + userIdCopy + musicId ,value  = favoriteRate * favoriteRateCopy
    // 就是计算两个不同的用户喜好同一首音乐的「喜爱程度」的乘积
    val musicRelatedDF = tmpMusicRelatedDF.selectExpr(RecallCFEnum.userId.toString, RecallCFEnum.favoriteRate.toString,
      RecallCFEnum.musicId.toString, CommonUtils.concatCopy(RecallCFEnum.userId.toString), CommonUtils.concatCopy(RecallCFEnum.favoriteRate.toString))
      .withColumn(RecallCFEnum.favoriteRateMolecular.toString, FunctionUDFUtils.productUdf(col(RecallCFEnum.favoriteRate.toString), col(CommonUtils.concatCopy(RecallCFEnum.favoriteRate.toString))))

    // 计算两个不同的用户喜好音乐的「喜爱程度」的乘积 的和
    // 这个结果相当于计算相似度公式中的 分子
    val tmp = musicRelatedDF
      .groupBy(RecallCFEnum.userId.toString, CommonUtils.concatCopy(RecallCFEnum.userId.toString))
      .agg("favoriteRateMolecular" -> "sum")
      .withColumnRenamed("sum(favoriteRateMolecular)", RecallCFEnum.molecular.toString)
    tmp
  }

  private def saveDataFrame2Hive(df: DataFrame, tableName: String): Unit = {
    val tmpView = s"tmp_user_recall_view"
    df.createOrReplaceTempView(tmpView)
    spark.sql(s"insert into $tableName partition(date='${LocalDate.now().toString}') select user_id,music_id,score from $tmpView")
  }
}
