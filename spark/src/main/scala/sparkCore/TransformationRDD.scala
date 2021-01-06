package sparkCore

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author liuwenyi
 * @date 2020/12/30
 */
object TransformationRDD extends App {

  val conf = new SparkConf().setMaster("local[*]").setAppName("TransformationRDD")
  val sc = new SparkContext(conf)
  val textRdd = sc.textFile("/Users/liuwenyi/IdeaProjects/satan/data/word.txt")

  // 设置检查点路径
  sc.setCheckpointDir("/Users/liuwenyi/IdeaProjects/satan/data/checkpoint_result")
  // 将某个 RDD 设置为检查点
  textRdd.checkpoint()
  var resultRdd = textRdd.flatMap(lines => lines.split(" "))
    .map(line => (line.toLowerCase, 1))
    .countByKey()
}
