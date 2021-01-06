package sparkCore

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author liuwenyi
 * @date 2021/01/04
 */
object WordCount {
  def wordCount(inputPath: String, outPutPath: String): Unit = {
    val conf = new SparkConf().setAppName("word_count").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val wordRdd = sc.textFile(inputPath)
    wordRdd.flatMap(lines => lines.split(" ")).map(line => (line, 1))
      .reduceByKey(_ + _)
      .saveAsTextFile(outPutPath)
  }

  def main(args: Array[String]): Unit = {
    val len = args.length
    println("****************" + len)
    var inputPath = ""
    var outputPath = ""
    if (len < 2) {
      throw new IllegalArgumentException("参数异常")
    }
    inputPath = args(0)
    outputPath = args(1)
    wordCount(inputPath, outputPath)
  }
}
