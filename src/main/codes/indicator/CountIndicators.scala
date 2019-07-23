package indicator

import conf.ConfigManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * 统计指标
  */
object CountIndicators {

  def main(args: Array[String]): Unit = {
//    fetchIndicatorFields().take(4).foreach(println)
  }

  /* 读取parquet文件 */
  def fetchFile(): DataFrame ={
    val sk: SparkSession = ConfigManager.fetchSparkSession()
    val df: DataFrame = sk.read
      // 可以把路径当作参数传进来
      .parquet("D:\\programs\\java_idea\\DMP\\src\\outPutFiles\\parquetFile")
    df
  }

  /* 获取所需字段 */
  def fetchIndicatorFields(): RDD[(Int, Int, Int, Int, Int, Int, Int)] ={
    val dfForIndicator: DataFrame = fetchFile()
    dfForIndicator.rdd.map(row => {
      // 数据请求方式（1:请求、2:展示、3:点击）
      val requestmode: Int = row.getAs[Int]("requestmode")
      // 流程节点（1：请求量 kpi 2：有效请求 3：广告请求）
      val processnode: Int = row.getAs[Int]("processnode")
      // 有效标识（有效指可以正常计费的）(0：无效 1：有效
      val iseffective: Int = row.getAs[Int]("iseffective")
      // 是否收费（0：未收费 1：已收费）
      val isbilling: Int = row.getAs[Int]("isbilling")
      // 是否竞价
      val isbid: Int = row.getAs[Int]("isbid")
      // 是否竞价成功
      val iswin: Int = row.getAs[Int]("iswin")
      // 广告id
      val adorderid: Int = row.getAs[Int]("adorderid")
      (requestmode, processnode, iseffective, isbilling, isbid, iswin, adorderid)
    })
  }

}
