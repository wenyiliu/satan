package sparkSql

import org.apache.spark.sql.SparkSession
import sparkCore.userLog.UserLogItem

import scala.util.Random

/**
 * @author liuwenyi
 * @date 2021/01/07
 */
object DataFrameTest {
  case class Emp(ename: String, comm: Double, deptno: Long, empno: Long,
                 hiredate: String, job: String, mgr: Long, sal: Double)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrameTest")
      .master("local[*]")
      .getOrCreate()
    // 1.需要导入隐式转换
    import spark.implicits._
    // 2.创建 case class, case class的定义要在引用case class函数的外面

    // 3.由内部数据集创建 Datasets
    val caseClassDS = Seq(Emp("ALLEN", 300.0, 30, 7499, "1981-02-20 00:00:00", "SALESMAN", 7698, 1600.0),
      Emp("JONES", 300.0, 30, 7499, "1981-02-20 00:00:00",
        "SALESMAN", 7698, 1600.0)).toDS()
    caseClassDS.show()

  }
}
