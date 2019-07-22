package utils

import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object UtilsForProj {

  /* 把每行的array转为Row，以便后续创建dataFrame */
  def makeRow(arrRdd: RDD[Array[String]]): RDD[Row] ={
    arrRdd.map(arr => {
      Row(
        arr(0),
        parseFieldToInt(arr(1)),
        parseFieldToInt(arr(2)),
        parseFieldToInt(arr(3)),
        parseFieldToInt(arr(4)),
        arr(5),
        arr(6),
        parseFieldToInt(arr(7)),
        parseFieldToInt(arr(8)),
        parseFieldToDouble(arr(9)),
        parseFieldToDouble(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        parseFieldToInt(arr(17)),
        arr(18),
        arr(19),
        parseFieldToInt(arr(20)),
        parseFieldToInt(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        parseFieldToInt(arr(26)),
        arr(27),
        parseFieldToInt(arr(28)),
        arr(29),
        parseFieldToInt(arr(30)),
        parseFieldToInt(arr(31)),
        parseFieldToInt(arr(32)),
        arr(33),
        parseFieldToInt(arr(34)),
        parseFieldToInt(arr(35)),
        parseFieldToInt(arr(36)),
        arr(37),
        parseFieldToInt(arr(38)),
        parseFieldToInt(arr(39)),
        parseFieldToDouble(arr(40)),
        parseFieldToDouble(arr(41)),
        parseFieldToInt(arr(42)),
        arr(43),
        parseFieldToDouble(arr(44)),
        parseFieldToDouble(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        parseFieldToInt(arr(57)),
        parseFieldToDouble(arr(58)),
        parseFieldToInt(arr(59)),
        parseFieldToInt(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        parseFieldToInt(arr(73)),
        parseFieldToDouble(arr(74)),
        parseFieldToDouble(arr(75)),
        parseFieldToDouble(arr(76)),
        parseFieldToDouble(arr(77)),
        parseFieldToDouble(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        parseFieldToInt(arr(84))
      )
    })
  }
  /* 创建structure*/
  def makeStructure(): StructType ={
    val struct: StructType = StructType(Array(
      StructField("sessionid", StringType),
      StructField("advertisersid", IntegerType),
      StructField("adorderid", IntegerType),
      StructField("adcreativeid", IntegerType),
      StructField("adplatformproviderid", IntegerType),
      StructField("sdkversion", StringType),
      StructField("adplatformkey", StringType),
      StructField("putinmodeltype", IntegerType),
      StructField("requestmode", IntegerType),
      StructField("adprice", DoubleType),
      StructField("adppprice", DoubleType),
      StructField("requestdate", StringType),
      StructField("ip", StringType),
      StructField("appid", StringType),
      StructField("appname", StringType),
      StructField("uuid", StringType),
      StructField("device", StringType),
      StructField("client", IntegerType),
      StructField("osversion", StringType),
      StructField("density", StringType),
      StructField("pw", IntegerType),
      StructField("ph", IntegerType),
      StructField("long", StringType),
      StructField("lat", StringType),
      StructField("provincename", StringType),
      StructField("cityname", StringType),
      StructField("ispid", IntegerType),
      StructField("ispname", StringType),
      StructField("networkmannerid", IntegerType),
      StructField("networkmannername", StringType),
      StructField("iseffective", IntegerType),
      StructField("isbilling", IntegerType),
      StructField("adspacetype", IntegerType),
      StructField("adspacetypename", StringType),
      StructField("devicetype", IntegerType),
      StructField("processnode", IntegerType),
      StructField("apptype", IntegerType),
      StructField("district", StringType),
      StructField("paymode", IntegerType),
      StructField("isbid", IntegerType),
      StructField("bidprice", DoubleType),
      StructField("winprice", DoubleType),
      StructField("iswin", IntegerType),
      StructField("cur", StringType),
      StructField("rate", DoubleType),
      StructField("cnywinprice", DoubleType),
      StructField("imei", StringType),
      StructField("mac", StringType),
      StructField("idfa", StringType),
      StructField("openudid", StringType),
      StructField("androidid", StringType),
      StructField("rtbprovince", StringType),
      StructField("rtbcity", StringType),
      StructField("rtbdistrict", StringType),
      StructField("rtbstreet", StringType),
      StructField("storeurl", StringType),
      StructField("realip", StringType),
      StructField("isqualityapp", IntegerType),
      StructField("bidfloor", DoubleType),
      StructField("aw", IntegerType),
      StructField("ah", IntegerType),
      StructField("imeimd5", StringType),
      StructField("macmd5", StringType),
      StructField("idfamd5", StringType),
      StructField("openudidmd5", StringType),
      StructField("androididmd5", StringType),
      StructField("imeisha1", StringType),
      StructField("macsha1", StringType),
      StructField("idfasha1", StringType),
      StructField("openudidsha1", StringType),
      StructField("androididsha1", StringType),
      StructField("uuidunknow", StringType),
      StructField("userid", StringType),
      StructField("iptype", IntegerType),
      StructField("initbidprice", DoubleType),
      StructField("adpayment", DoubleType),
      StructField("agentrate", DoubleType),
      StructField("lomarkrate", DoubleType),
      StructField("adxrate", DoubleType),
      StructField("title", StringType),
      StructField("keywords", StringType),
      StructField("tagid", StringType),
      StructField("callbackdate", StringType),
      StructField("channelid", StringType),
      StructField("mediatype", IntegerType)
    ))
    struct

  }
  /* 把字段中的字符串或者空串转为int类型返回 */
  def parseFieldToInt(field: String): Int ={
    try{
      field.toInt
    }catch {
      case _ : Exception => 0
    }
  }
  /* 把字段中的字符串或者空串转为double类型返回 */
  def parseFieldToDouble(field: String): Double ={
    try{
      field.toDouble
    }catch {
      case _ : Exception => 0.0
    }
  }
  /* 把数据存入mysql */
  def loadToMysql(ct_prov_city_df: DataFrame): Unit ={
    val url: String = "jdbc:mysql://localhost:3306/dmp"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "1234")
    ct_prov_city_df.write.jdbc(url, "dmp_table", properties)
  }
  /* 以json形式存入文件 */
  def loadToJsonFile(ct_prov_city_df: DataFrame): Unit ={
    ct_prov_city_df.write
//      .partitionBy("provincename", "cityname")
      .json("D:\\programs\\java_idea\\DMP\\src\\outPutFiles\\out")
  }


}
