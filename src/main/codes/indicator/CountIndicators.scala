package indicator

import conf.ConfigManager
import indicatorUtils.IndicatorsUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


/**
  * 统计指标
  */
object CountIndicators {
//  val sk: SparkSession = ConfigManager.fetchSparkSession()
  val df: DataFrame = fetchFile()
  val appDictBr: Broadcast[Map[String, String]] = fetchAppDict()

  def main(args: Array[String]): Unit = {
    test()

  }

  // 创建 app 映射文件
  def create_appdict_file(): Unit ={
    val ssc: SparkSession = ConfigManager.fetchSparkSession()
    val lines: RDD[String] = ssc.sparkContext.textFile("D:\\programs\\java_idea\\DMP\\src\\files\\app_dict.txt")
    lines
      .map(e => e.split("\t"))  // 对字符串进行分割
      .filter(_.length >= 5)  // 取出长度大于等于5 的
      .map(t => (t(1).trim, t(4).trim))  // 取出名字和url
      .saveAsTextFile("D:\\programs\\java_idea\\DMP\\src\\outPutFiles\\app_dict")


  }

  /* 读取app映射文件 */
  def fetchAppDict(): Broadcast[Map[String, String]] ={
      val ssc: SparkSession = ConfigManager.fetchSparkSession()
    import ssc.implicits._
      val lines: RDD[String] = ssc
        .sparkContext.textFile("D:\\programs\\java_idea\\DMP\\src\\outPutFiles\\app_dict")
      val br: Map[String, String] = lines.map(e => {
        try{
          (e.substring(1, e.indexOf(",")),e.substring(e.indexOf(",")+1, e.indexOf(")")))
        }catch {
          case t : StringIndexOutOfBoundsException => ("wrong", "wrong")
        }
      }).collect().toMap
    val brcast: Broadcast[Map[String, String]] = ssc.sparkContext.broadcast(br)
    brcast
  }

  /* 读取parquet文件 */
  def fetchFile(): DataFrame ={
    val ssc: SparkSession = ConfigManager.fetchSparkSession()
    val df: DataFrame = ssc.read
      // 可以把路径当作参数传进来
      .parquet("D:\\programs\\java_idea\\DMP\\src\\outPutFiles\\parquetFile")
    df
  }

  /* 按照不同key统计指标 */
  def indicatorsOfKeys(): Unit ={
    // 按地域统计指标
    val areaKeyList: List[String] = List[String]("provincename", "cityname")
    IndicatorsUtils.indicatorsOfKeylist(df, areaKeyList)
    // 按照运营商统计指标
    val ispKeyList: List[String] = List[String]("ispname")
    IndicatorsUtils.indicatorsOfKeylist(df, ispKeyList)
    // 按照网络统计指标
    val netKeyList: List[String] = List[String]("networkmannername")
    IndicatorsUtils.indicatorsOfKeylist(df, netKeyList)


  }

  /* 地域分布指标 sql 版 */
  def indicatorsOfArea_SQL(): Unit ={
    df.createTempView("vv")
    df.sqlContext.sql(
      "" +
        "select " +
        "temp.provincename, " +
        "temp.cityname, " +
        "sum(temp.n1)," +
        "sum(temp.n2)," +
        "sum(temp.n3)," +
        "sum(temp.n4)," +
        "sum(temp.n5)," +
        "sum(temp.n6)," +
        "sum(temp.n7)," +
        "sum(temp.n8)," +
        "sum(temp.n9) from " +
        "(select " +
        "provincename as provincename, " +
        "cityname as cityname, " +
        "if(requestmode == 1 and processnode >= 1,1,0) as n1," +
        "if(requestmode == 1 and processnode >= 2,1,0) as n2," +
        "if(requestmode == 1 and processnode == 3,1,0) as n3," +
        "if(iseffective == 1 and isbilling == 1 and isbid == 1,1,0) as n4," +
        "if(iseffective == 1 and isbilling == 1 and isbid == 1 and iswin == 1 and adorderid != 0,1,0) as n5," +
        "if(requestmode == 2 and iseffective == 1,1,0) as n6," +
        "if(requestmode == 3 and iseffective == 1,1,0) as n7," +
        "if(iseffective == 1 and isbilling == 1 and iswin == 1,1,0) as n8," +
        "if(iseffective == 1 and isbilling == 1 and iswin == 1,1,0) as n9 " +
        "from vv) temp " +
        "group by " +
        "temp.provincename, temp.cityname limit 5"
    ).show()
  }
  /* 设备分类指标 sql 版 */
  def indicatorsOfDevicetype_SQL(): Unit ={
    df.createTempView("vv")
    df.sqlContext.sql(
      "" +
        "select " +
        "temp.device, " +
        "sum(temp.n1)," +
        "sum(temp.n2)," +
        "sum(temp.n3)," +
        "sum(temp.n4)," +
        "sum(temp.n5)," +
        "sum(temp.n6)," +
        "sum(temp.n7)," +
        "sum(temp.n8)," +
        "sum(temp.n9) from " +
        "(select " +
        "case devicetype " +
        "when 1 then '手机' " +
        "when 2 then '平板' " +
        "else '其他' " +
        "end as device," +
        "if(requestmode == 1 and processnode >= 1,1,0) as n1," +
        "if(requestmode == 1 and processnode >= 2,1,0) as n2," +
        "if(requestmode == 1 and processnode == 3,1,0) as n3," +
        "if(iseffective == 1 and isbilling == 1 and isbid == 1,1,0) as n4," +
        "if(iseffective == 1 and isbilling == 1 and isbid == 1 and iswin == 1 and adorderid != 0,1,0) as n5," +
        "if(requestmode == 2 and iseffective == 1,1,0) as n6," +
        "if(requestmode == 3 and iseffective == 1,1,0) as n7," +
        "if(iseffective == 1 and isbilling == 1 and iswin == 1,1,0) as n8," +
        "if(iseffective == 1 and isbilling == 1 and iswin == 1,1,0) as n9 " +
        "from vv) temp " +
        "group by " +
        "temp.device limit 5"
    ).show()
  }
  /* 系统分类指标 sql 版 */
  def indicatorsOfOs_SQL(): Unit ={
    df.createTempView("vv")
    df.sqlContext.sql(
      "" +
        "select " +
        "temp.os, " +
        "sum(temp.n1)," +
        "sum(temp.n2)," +
        "sum(temp.n3)," +
        "sum(temp.n4)," +
        "sum(temp.n5)," +
        "sum(temp.n6)," +
        "sum(temp.n7)," +
        "sum(temp.n8)," +
        "sum(temp.n9) from " +
        "(select " +
        "case client " +
        "when 1 then 'android' " +
        "when 2 then 'ios' " +
        "else 'others' " +
        "end as os," +
        "if(requestmode == 1 and processnode >= 1,1,0) as n1," +
        "if(requestmode == 1 and processnode >= 2,1,0) as n2," +
        "if(requestmode == 1 and processnode == 3,1,0) as n3," +
        "if(iseffective == 1 and isbilling == 1 and isbid == 1,1,0) as n4," +
        "if(iseffective == 1 and isbilling == 1 and isbid == 1 and iswin == 1 and adorderid != 0,1,0) as n5," +
        "if(requestmode == 2 and iseffective == 1,1,0) as n6," +
        "if(requestmode == 3 and iseffective == 1,1,0) as n7," +
        "if(iseffective == 1 and isbilling == 1 and iswin == 1,winprice/1000.0,0) as n8," +
        "if(iseffective == 1 and isbilling == 1 and iswin == 1,adpayment/1000.0,0) as n9 " +
        "from vv) temp " +
        "group by " +
        "temp.os limit 100"
    ).show(5)
  }



  // 单元测试
  def test(): Unit ={
    df.createTempView("vv")
    df.sqlContext.sql("select adplatformproviderid from vv limit 20").show(200)
//    val netKeyList: List[String] = List[String]("appname")
//    IndicatorsUtils.indicatorsOfKeylist(df, netKeyList)
//    indicatorsOfOs_SQL()
  }
}
