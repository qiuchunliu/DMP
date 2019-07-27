import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Examination {

  private val ssc: SparkSession = SparkSession.builder().master("local[4]").appName("exam").getOrCreate()

  def main(args: Array[String]): Unit = {
    val lines: RDD[String] =
      ssc
        .sparkContext
        .textFile("D:\\programs\\java_idea\\DMP\\src\\main\\exam726\\json.txt")


    var list1: List[(String, Int)] = List[(String, Int)]()
    var list2: List[(String, Int)] = List[(String, Int)]()

    /*
     * 获取内部 jsonarray 的值
     */
    val res: Array[JSONArray] = lines.mapPartitions(p => {
      p.map(e => {
        // json 解析一个json串
        JSON.parseObject(e)
      }).filter(t => {
        // 过滤掉状态码不是 1 的串
        t.getString("status").equals("1")
      }).map(e => {
        // 获取内部的json串并转为jsonobject
        e.getJSONObject("regeocode")
      }).map(e => {
        // 获取内部 pois 数据
        // 返回一个数组，内部是各个 json 串
        e.getObject("pois", classOf[JSONArray])
      })
    }).collect()

    /*
     * 对每个jsonarray进行提取所需的数据
     */
    for (i <- res) {
      /*
       * 调用方法处理 JSONArray
       * 返回一个列表
       * 最后将每个返回的列表加一起
       * 返回一个最终的列表
       */
      for (j <- parseJsonArray(i)) {
        list1 :+= j._1
        list2 ++= j._2
      }
    }

    val res1: Map[String, Int] = list1.filter(!_._1.equals("[]")).groupBy(_._1).mapValues(_.foldLeft(0)(_+_._2))
    val res2: Map[String, Int] = list2.filter(!_._1.equals("[]")).groupBy(_._1).mapValues(_.foldLeft(0)(_+_._2))
    res1.foreach(println)
    res2.foreach(println)
  }


  /**
    * 处理 jsonArray
    * 返回一个集合
    * @param e 需要处理的jsonarray
    */
  def parseJsonArray(e: JSONArray): List[((String, Int), List[(String, Int)])] ={

    var list: List[((String, Int), List[(String, Int)])] = List[((String, Int), List[(String, Int)])]()

    for (i <- 0 until e.size()) {
      // 获取数组中的每一个JSONObject
      val obj: JSONObject = e.getJSONObject(i)
//      // 用户id
//      // 但是不用统计
//      val id: String = obj.getString("id")

      // 位置信息
      val businessarea: String = obj.getString("businessarea")
      // 商圈， 需要进行切分
      val tp: String = obj.getString("type")
      val tuples: List[(String, Int)] = tpSplit(tp)

      list :+= ((businessarea, 1), tuples)
    }
    list
  }

  def tpSplit(tp: String): List[(String, Int)] ={
    var list: List[(String, Int)] = List[(String, Int)]()
    if (StringUtils.isNotBlank(tp)){
      if (tp.contains(";")) {
        for (a <- tp.split(";")){
          list :+= (a, 1)
        }
      }
      else {
        list :+= (tp, 1)
      }
    }
    list

  }


}
