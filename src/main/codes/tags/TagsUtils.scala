package tags

import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row}
import tags.TagsMain.{df, ssc}

object TagsUtils {

  // 过滤条件
  val userIdOne: String =
    """
      |imei !='' or mac!='' or idfa != '' or openudid!='' or androidid!='' or
      |imeimd5 !='' or macmd5!='' or idfamd5 != '' or openudidmd5!='' or androididmd5!='' or
      |imeisha1 !='' or macsha1!='' or idfasha1 != '' or openudidsha1!='' or androididsha1!=''
    """.stripMargin

  /* 用户id标签 */
  def tagsUserId(row: Row): List[(String, Int)]={

    var list: List[(String, Int)] = List[(String, Int)]()
    row match {
      case r if StringUtils.isNotBlank(r.getAs[String]("imei")) =>
        list :+= (r.getAs[String]("imei"), 1)
      case r if StringUtils.isNotBlank(r.getAs[String]("mac")) =>
        list :+= (r.getAs[String]("mac"), 1)
      case r if StringUtils.isNotBlank(r.getAs[String]("idfa")) =>
        list :+= (r.getAs[String]("idfa"), 1)
      case r if StringUtils.isNotBlank(r.getAs[String]("openudid")) =>
        list :+= (r.getAs[String]("openudid"), 1)
      case r if StringUtils.isNotBlank(r.getAs[String]("androidid")) =>
        list :+= (r.getAs[String]("androidid"), 1)
      case r if StringUtils.isNotBlank(r.getAs[String]("imeimd5")) =>
        list :+= (r.getAs[String]("imeimd5"), 1)
      case r if StringUtils.isNotBlank(r.getAs[String]("macmd5")) =>
        list :+= (r.getAs[String]("macmd5"), 1)
      case r if StringUtils.isNotBlank(r.getAs[String]("idfamd5")) =>
        list :+= (r.getAs[String]("idfamd5"), 1)
      case r if StringUtils.isNotBlank(r.getAs[String]("openudidmd5")) =>
        list :+= (r.getAs[String]("openudidmd5"), 1)
      case r if StringUtils.isNotBlank(r.getAs[String]("androididmd5")) =>
        list :+= (r.getAs[String]("androididmd5"), 1)
      case r if StringUtils.isNotBlank(r.getAs[String]("imeisha1")) =>
        list :+= (r.getAs[String]("imeisha1"), 1)
      case r if StringUtils.isNotBlank(r.getAs[String]("idfasha1")) =>
        list :+= (r.getAs[String]("idfasha1"), 1)
      case r if StringUtils.isNotBlank(r.getAs[String]("macidsha1")) =>
        list :+= (r.getAs[String]("macidsha1"), 1)
      case r if StringUtils.isNotBlank(r.getAs[String]("openudidsha1")) =>
        list :+= (r.getAs[String]("openudidsha1"), 1)
      case r if StringUtils.isNotBlank(r.getAs[String]("androididsha1")) =>
        list :+= (r.getAs[String]("androididsha1"), 1)
    }
    list
  }

  /* 广告位标签 */
  def tagsAdType(row: Row): List[(String, Int)] ={
    var list: List[(String, Int)] = List[(String, Int)]()
    row.getAs[Int]("adspacetype") match {
      case n if n <= 9 && n >= 0 => list :+= ("LC0" + n, 1)
      case n if n > 9 => list :+= ("LC" + n, 1)
    }
    if (StringUtils.isBlank(row.getAs[String]("adspacetypename"))){
      list :+= ("LN" + row.getAs[String]("adspacetypename"), 1)
    }
    list
  }

  /* 渠道标签 */
  def tagsChannel(row: Row): List[(String, Int)] ={
    //adplatformproviderid
    var list: List[(String, Int)] = List[(String, Int)]()
    list :+= ("CN" + row.getAs[Int]("adplatformproviderid"), 1)
    list
  }

  /**
    * 设备标签，包括
    * 操作系统 client int
    * 联网方式 networkmannername string
    * 运营商 ispname string
    */
  def tagsDevice(row: Row): List[(String, Int)] ={
    var list: List[(String, Int)] = List[(String, Int)]()
    // 操作系统
    row.getAs[Int]("client") match {
      case 1 => list :+= ("D00010001", 1)
      case 2 => list :+= ("D00010002", 1)
      case 3 => list :+= ("D00010003", 1)
      case _ => list :+= ("D00010004", 1)
    }
    // 联网方式
    row.getAs[String]("networkmannername") match {
      case s if s.equalsIgnoreCase("wifi") =>
        list :+= ("D00020001", 1)
      case s if s.equalsIgnoreCase("2G") =>
        list :+= ("D00020004", 1)
      case s if s.equalsIgnoreCase("3G") =>
        list :+= ("D00020003", 1)
      case s if s.equalsIgnoreCase("4G") =>
        list :+= ("D00020002", 1)
      case _ => list :+= ("D00020005", 1)
    }
    // 运营商
    row.getAs[String]("ispname") match {
      case s if s.equals("移动") => list :+= ("D00030001", 1)
      case s if s.equals("联通") => list :+= ("D00030002", 1)
      case s if s.equals("电信") => list :+= ("D00030003", 1)
      case _ => list :+= ("D00030004", 1)
    }
    list
  }

  /**
    * 关键字标签  keywords
    * 每个关键字不能小于3个字符，不能大于 8个字符
    * 还要过滤不采用的关键字
    */
  def tagsKeyWords(row: Row, br: Broadcast[Array[String]])
  : List[(String, Int)] ={
    var list: List[(String, Int)] = List[(String, Int)]()
    val str: String = row.getAs[String]("keywords")
    if (str.contains("|")){

      str
        .split("\\|")
        .filter(e => {e.length >= 3 && e.length <= 8})
        .filter(!br.value.contains(_))  // 要加过滤
        .foreach(e => {list :+= ("K" + e, 1)})

    }
    list
  }

  /**
    * 地域标签  "provincename", "cityname"
    * 包括 省
    * 包括 市
    */
  def tagsProvCity(row: Row): List[(String, Int)] ={
    var list: List[(String, Int)] = List[(String, Int)]()
    if (StringUtils.isNotBlank(row.getAs[String]("provincename"))) {
      list:+= ("ZP" + row.getAs[String]("provincename"), 1)
    }
    if (StringUtils.isNotBlank(row.getAs[String]("cityname"))) {
      list:+= ("ZC" + row.getAs[String]("cityname"), 1)
    }
    list
  }

  /**
    * business
    * 商圈标签
    * 通过经纬度字段获取商圈的名称
    * 并保存
    */
  def tagsBusiness(df: DataFrame): Unit ={
    import ssc.implicits._
    df.filter(TagsUtils.userIdOne).map(row => {
        // 获取经纬度
        val longitude: String = row.getAs[String]("long")
        // 获取维度
        val latitude: String = row.getAs[String]("lat")
      (longitude, latitude)
      }
    ).rdd.filter(e => {  // 此处过滤 空值 有问题，后续解决
      !e._1.equals("0") && !e._2.equals("0")
    }).take(50).foreach(println)
  }

}
