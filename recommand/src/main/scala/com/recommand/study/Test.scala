package com.recommand.study


/**
 * @author liuwenyi
 * @date 2021/01/14
 */
object Test {
  def main(args: Array[String]): Unit = {
    //    val spark = SparkSessionUtils.getDefaultSession
    //    import spark.implicits._
    //    val data = spark.sparkContext.parallelize(Array("1_a_5", "2_b_4", "1_c_4", "1_b_4", "2_a_4")).map(x => {
    //      val values = x.split("_")
    //      (values(0), values(1), values(2))
    //    }).toDF("id", "name", "score")
    //    val dataCopy = CommonUtils.copyDF(data)
    //    dataCopy.show()
    //    dataCopy.join(data, dataCopy("nameCopy") === data("name"))
    //      .filter("id <> idCopy")
    //      .selectExpr("id", "idCopy", "score * scoreCopy as scoreSum")
    //      .groupBy("id", "idCopy")
    //      .agg("scoreSum" -> "sum")
    //      .withColumnRenamed("sum(scoreSum)", "totalScore")
    //      .show()
    val a = Seq(1, 2, 3)
    val b = Seq(1, 3)

    println(a diff  b)
  }

}
