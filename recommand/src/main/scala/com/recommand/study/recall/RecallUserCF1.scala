package com.recommand.study.recall

import breeze.numerics.{pow, sqrt}
import com.recommand.study.sql.HiveSql
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * @author liuwenyi
 * @date 2021/01/10
 */
object RecallUserCF1 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("recall user cf")
      .enableHiveSupport()
      .getOrCreate()
    userItemRate(spark)
  }

  def userItemRate(spark: SparkSession): DataFrame = {
    val user_listen = spark.sql(HiveSql.select_all_user_listen)
    val userItemTotalTime = user_listen.selectExpr("userId", "musicId", "cast(remainTime as double)")
      .groupBy("userId", "musicId")
      .sum("remainTime")
      .withColumnRenamed("sum(remainTime)", "itemTotalTime")
    val userTotalTime = user_listen.selectExpr("userId", "musicId", "cast(remainTime as double)")
      .groupBy("userId")
      .sum("remainTime")
      .withColumnRenamed("sum(remainTime)", "totalTime")
    val user_item = userItemTotalTime.join(userTotalTime, "userId")
      .selectExpr("userId", "musicId as itemId", "itemTotalTime/totalTime as itemRate")
    user_item.createOrReplaceTempView("user_item")
    //    spark.sql("select * from user_item limit 100 ").show(100)
    import spark.implicits._
    val userSumPowRate = user_item.rdd
      .map(userItem => (userItem(0).toString, userItem(2).toString))
      .groupByKey()
      .mapValues(value => sqrt(value.toArray.map(rate => pow(rate.toDouble, 2)).sum))
      .toDF("userId", "sqrtRateSum")
    userSumPowRate.cache()
    //    userSumPowRating.show(3)
    val user_item_copy = user_item.selectExpr("userId as userIdCopy",
      "itemId as itemIdCopy",
      "itemRate as itemRateCopy")
    val userItem2Item = user_item.join(user_item_copy,
      user_item("itemId") === user_item_copy("itemIdCopy"))
      .filter("userId <> userIdCopy")
      .withColumn("rating_product", col("itemRate") * col("itemRateCopy"))
      .groupBy("userId", "userIdCopy")
      .agg("rating_product" -> "sum")
      .withColumnRenamed("sum(rating_product)", "rate_dot")
    val userSumPowRateCopy = userSumPowRate.selectExpr("userId as userIdCopy", "sqrtRateSum as sqrtRateSumCopy")
    val userSim = userItem2Item.join(userSumPowRate, "userId")
      .join(userSumPowRateCopy, "userIdCopy")
      .selectExpr("userId", "userIdCopy", "rate_dot/(sqrtRateSumCopy * sqrtRateSum) as cos_sim")
    userSim.show(100)
    user_item
  }
}
