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
    val ds: RDD[(String, List[(String, Int)])] = fetchTags()
    ds.take(500).foreach(println)

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

  def fetchTags(): RDD[(String, List[(String, Int)])] ={
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

      (userIdTags, adTags ++ channelTags ++ deviceTags ++ keyWordsTags ++ proCityTags)
    })
      .map(x => {  // 对每一条的数据的重复字段进行聚合
      (x._1, x._2.groupBy(_._1).mapValues(_.size).toList)
    })
      .rdd.reduceByKey((l1, l2) =>
      (l1 ::: l2).groupBy(_._1).mapValues(a => {a.map(aa => aa._2).sum}).toList
      // 如果用 .size 逻辑不对的
//      (l1 ::: l2).groupBy(_._1).mapValues(_.size).toList
    )
      .map(a => (a._1.head._1, a._2))
  }

}
