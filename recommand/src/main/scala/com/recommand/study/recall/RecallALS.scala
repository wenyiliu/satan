package com.recommand.study.recall

import com.recommand.study.bean.ALSParam
import com.recommand.study.sql.HiveSql
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.types.{LongType, StructField}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 * @author liuwenyi
 * @date 2021/01/18
 */
class RecallALS(spark: SparkSession, param: ALSParam) extends BaseRecallCF {

  import spark.implicits._

  private var tableName = ""

  def this(spark: SparkSession, param: ALSParam, tableName: String) {
    this(spark, param)
    this.tableName = tableName
  }

  def aLSResult(): Unit = {
    val userMusicFavoriteRateCF = getUserMusicFavoriteRate(spark)
    val user_item_rating = transFormIdToInt(userMusicFavoriteRateCF, "userId", "musicId", spark)
    val Array(train, test) = user_item_rating.randomSplit(Array(0.8, 0.2), seed = 1)
    val model = getALSModel(param, train)
    // 删除预测值为 Nan 的记录
    model.setColdStartStrategy("drop")
    val unListen = getUserUnListen(user_item_rating)
    val resultDF = model.transform(unListen)
      .selectExpr("userId", "iid as musicId", "prediction as score")
      .withColumn("rank", row_number().over(Window.partitionBy("userId").orderBy($"score".desc)))
      .filter(s"rank<=30")
      .drop("rank")
    if (tableName == null || tableName == "") {
      resultDF.show()
    } else {
      resultDF.createOrReplaceTempView("als_tem_table")
      spark.sql(HiveSql.save_recall_table_sql(tableName, "als_tem_table"))
    }
    println(getRMSE(model, test))
  }


  def transFormIdToInt(data: DataFrame, userId: String, musicId: String, spark: SparkSession): DataFrame = {
    val data_with_UserId = data.groupBy(userId).count().selectExpr(userId + " as userId1", "count")
    val temp_uid_data = inreaseId(data_with_UserId, spark, "uid")
    //为 iid 转化类型
    val data_with_ItemId = data.groupBy(musicId).count().selectExpr(musicId + " as musicId1", "count")
    val temp_iid_data = inreaseId(data_with_ItemId, spark, "iid")
    //join 数据
    data.join(temp_uid_data, $"userId" === $"userId1").
      join(temp_iid_data, $"musicId" === $"musicId1").
      selectExpr("userId", "uid", "iid", "favoriteRate as rating")
  }

  private def getAllItem(df: Dataset[Row]): Set[String] = {
    df.groupBy("iid").count()
      .filter($"count" > 5)
      .selectExpr("iid")
      .rdd.map(x => x(0).toString).
      collect()
      .toSet
  }

  def getUserUnListen(df: DataFrame): DataFrame = {
    import spark.implicits._
    // 获取被听过五次的音乐
    val item_set: Set[String] = getAllItem(df)
    df.rdd.map(x => (x(0) + "_" + x(1), x(2).toString))
      .groupByKey()
      .map(y => {
        (item_set -- y._2.toSet[String]).take(20)
          .map(z => y._1 + "_" + z)
      })
      .flatMap(m => m)
      .map(x => {
        val arr = x.split("_")
        (arr(0), arr(1).toInt, arr(2).toInt)
      }).toDF("userId", "uid", "iid")
  }

  def getRMSE(model: ALSModel, testDS: Dataset[_]): Double = {
    val evals = new RegressionEvaluator()
      .setMetricName("rmse")
      .setLabelCol("rating")
      .setPredictionCol("prediction")
    val predict_test = model.transform(testDS)
    evals.evaluate(predict_test)
  }

  private def inreaseId(df: DataFrame, spark: SparkSession, add_id: String): DataFrame = {
    val a = df.schema.add(StructField(add_id, LongType))
    val b = df.rdd.zipWithIndex()
    val c = b.map(tp => Row.merge(tp._1, Row(tp._2)))
    val new_df = spark.createDataFrame(c, a)
    new_df
  }

  private def getALSModel(param: ALSParam, dataSet: Dataset[_]): ALSModel = {
    assert(param != null, "param is not null")
    val asl = new ALS()
      .setMaxIter(param.maxIter)
      .setRegParam(param.regParam)
      .setRank(param.rank)
      .setUserCol(param.uid)
      .setItemCol(param.iid)
      .setRatingCol(param.rating)
    asl.fit(dataSet)
  }
}
