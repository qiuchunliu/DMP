package indicatorUtils

import indicator.CountIndicators
import org.apache.commons.lang.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object IndicatorsUtils {

  /* 获取values字段 */
  def fetchIndicatorsValues(dfForIndicator: DataFrame, ks: List[String])
  : RDD[(List[String], (Int, Int, Int, Int, Int, Int, Int, Double, Double))] ={
    dfForIndicator.rdd.map(row => {
      var keys: List[String] = List[String]()
      // 如果判断的是媒体分析的指标
      if (ks.size == 1 && ks.head.equalsIgnoreCase("appname")){
        keys = ks.map(e => {
          var appname: String = row.getAs[String](e)
          if (StringUtils.isEmpty(appname)){
            val appid: String = row.getAs[String]("appid")
            appname = fixNullValue(appid)
          }
          appname
        })
      }else{
        // 如果判断的不是媒体分析的指标
        keys = ks.map(e => row.getAs[String](e))
      }
      // 数据请求方式（1:请求、2:展示、3:点击）
      val requestmode: Int = row.getAs[Int]("requestmode")  // 1
      // 流程节点（1：请求量 kpi 2：有效请求 3：广告请求）
      val processnode: Int = row.getAs[Int]("processnode")  // 2
      // 有效标识（有效指可以正常计费的）(0：无效 1：有效
      val iseffective: Int = row.getAs[Int]("iseffective") // 3
      // 是否收费（0：未收费 1：已收费）
      val isbilling: Int = row.getAs[Int]("isbilling") // 4
      // 是否竞价
      val isbid: Int = row.getAs[Int]("isbid")  // 5
      // 是否竞价成功
      val iswin: Int = row.getAs[Int]("iswin")  // 6
      // 广告id
      val adorderid: Int = row.getAs[Int]("adorderid")  // 7
      // 广告消费
      val winprice: Double = row.getAs[Double]("winprice")  // 8
      // 广告成本
      val adpayment: Double = row.getAs[Double]("adpayment")  // 9
      // 返回元组
      (keys,
        (requestmode, processnode,
        iseffective, isbilling, isbid, iswin, adorderid,
      winprice, adpayment))
    })

  }

  /* values字段转list */
  def valuesToList(values: RDD[(List[String], (Int, Int, Int, Int, Int, Int, Int, Double, Double))])
  : RDD[(List[String], List[Double])] ={

    values.map(e => {
      // 原始请求数
      val n1: Int = if (e._2._1 == 1 && e._2._2 >= 1) 1 else 0
      val n2: Int = if (e._2._1 == 1 && e._2._2 >= 2) 1 else 0
      val n3: Int = if (e._2._1 == 1 && e._2._2 == 3) 1 else 0
      val n4: Int = if (e._2._3 == 1 && e._2._4 == 1 && e._2._5 == 1) 1 else 0
      val n5: Int = if (e._2._3 == 1 && e._2._4 == 1 && e._2._5 == 1 && e._2._6 == 1 && e._2._7 != 0) 1 else 0
      val n6: Int = if (e._2._1 == 2 && e._2._3 == 1) 1 else 0
      val n7: Int = if (e._2._1 == 3 && e._2._3 == 1) 1 else 0
      val n8: Int = if (e._2._3 == 1 && e._2._4 == 1 && e._2._6 == 1) 1 else 0
      val n9: Int = if (e._2._3 == 1 && e._2._4 == 1 && e._2._6 == 1) 1 else 0
      (e._1, List[Double](n1, n2, n3, n4, n5, n6, n7, n8, n9))
    })
  }

  /* 统计不同key的指标 */
  def indicatorsOfKeylist(df: DataFrame, keyList: List[String]): Unit ={
    val values: RDD[(List[String], (Int, Int, Int, Int, Int, Int, Int, Double, Double))] =
      IndicatorsUtils.fetchIndicatorsValues(df, keyList)
    IndicatorsUtils.valuesToList(values).reduceByKey((l1, l2) => {
      l1.zip(l2).map(e => {
        e._1 + e._2
      })
    })
      .map(t => {
        (t._1.toArray.mkString(","),  // 把key的list形式拆开
          t._2.head.toInt,
          t._2(1).toInt,
          t._2(2).toInt,
          t._2(3).toInt,
          t._2(4).toInt,
          t._2(5).toInt,
          t._2(6).toInt,
          t._2(7),
          t._2(8))
    })
      .take(40).foreach(println)
  }

  /**
    * 处理媒体分析指标
    * 如果媒体分析中，查到了空的key值
    * 用字典文件补充
    * @param appid 空值对应的id
    * @return 根据id查到的appname
    */
  def fixNullValue(appid: String): String ={
    val appDicts: Broadcast[RDD[(String, String)]] = CountIndicators.fetchAppDict()
    // 根据id查对应的appname
    appDicts.value.collect().toMap.getOrElse("appid", "others")
  }

}
