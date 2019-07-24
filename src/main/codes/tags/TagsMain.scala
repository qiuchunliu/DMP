package tags

import conf.ConfigManager
import indicator.CountIndicators
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 按指标打标签
  */
object TagsMain {

  /**
    * df 源文件
    * br 广播变量，app和id的键值对
    */
  val ssc: SparkSession = ConfigManager.fetchSparkSession()
  private val df: DataFrame = CountIndicators.df
  private val br: Broadcast[Array[String]] = fetchKeysFilter()

  def main(args: Array[String]): Unit = {
    val ds: Dataset[List[(String, Int)]] = fetchTags()
    ds.rdd.take(100).foreach(println)

  }

  /**
    * 读取关键字过滤文件
    * @return
    */
  def fetchKeysFilter(): Broadcast[Array[String]] ={
    val keysFilter: RDD[String] = ssc.sparkContext.textFile("D:\\programs\\java_idea\\DMP\\src\\files\\keyWordsFilter.txt")
    val filters: Array[String] = keysFilter.collect()
    ssc.sparkContext.broadcast(filters)
  }

  def fetchTags(): Dataset[List[(String, Int)]] ={
    import ssc.implicits._
    df.filter(TagsUtils.userIdOne).map(row => {
      // 用户id标签
      val userIdTags: List[(String, Int)] = TagsUtils.tagsUserId(row)
      // 广告标签
      val adTags: List[(String, Int)] = TagsUtils.tagsAdType(row)

      /* 渠道标签 */
      val channelTags: List[(String, Int)] = TagsUtils.tagsChannel(row)

      /* 设备标签 */
      val deviceTags: List[(String, Int)] = TagsUtils.tagsDevice(row)

      /* 关键字标签 */
      val keyWordsTags: List[(String, Int)] = TagsUtils.tagsKeyWords(row, br)

      /* 地域省市标签 */
      val proCityTags: List[(String, Int)] = TagsUtils.tagsProvCity(row)

      userIdTags ++ adTags ++ channelTags ++ deviceTags ++ keyWordsTags ++ proCityTags
    })
  }

}
