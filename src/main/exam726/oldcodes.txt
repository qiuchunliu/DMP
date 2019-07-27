import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Examination {

  private val ssc: SparkSession = SparkSession.builder().master("local[4]").appName("exam").getOrCreate()

  def main(args: Array[String]): Unit = {
    val lines: RDD[String] = ssc.sparkContext.textFile("D:\\programs\\java_idea\\DMP\\src\\main\\exam726\\json.txt")
    val jsonObs: RDD[JSONArray] = lines.map(l => {
      JSON.parseObject(l)
    }).filter(_.getString("status").equals("1")).map(e => {
      e.getObject("regeocode", classOf[JSONObject]).getObject("pois", classOf[JSONArray])
    })
    val t: RDD[Map[String, Int]] = jsonObs.map(e => {
      ff(e)
    })
    var list: List[(String, Int)] = List[(String, Int)]()
    val tem: Array[List[(String, Int)]] = t.map(e => {
      list ++= e.toList
      list
    }).collect()
    var lis: List[(String, Int)] = List[(String, Int)]()
    for (i <- tem){
      lis ++= i
    }
    val lll: List[(String, Int)] = lis.groupBy(_._1).mapValues(e => {e.map(ee => ee._2).sum}).toList
    lll.foreach(println)


    var li: List[(String, Int)] = List[(String, Int)]()
    var ll: List[(String, Int)] = List[(String, Int)]()

    val temp: Array[List[(String, Int)]] = jsonObs.map(ee => {
      val lis: List[(String, Int)] = f(ee)
      li ++= lis
      li
    }).collect()
    for (i <- temp) {
      ll ++= i
    }
    val res: List[(String, Int)] = ll.groupBy(_._1).mapValues(e => {e.map(ee => ee._2).sum}).toList
    res.foreach(e => {
      if(!e._1.equals("[]")){
        println(e)
      }
    })


  }


  def f(e: JSONArray): List[(String, Int)] ={
    var list: List[(String, Int)] = List[(String, Int)]()
    for (i <- 0 until e.size()){
      val obj: JSONObject = e.getObject(i, classOf[JSONObject])
//      val id: String = obj.getString("id")
      val businessarea: String = obj.getString("businessarea")
      list :+= (businessarea, 1)
    }
    list
  }

  def ff(e: JSONArray): Map[String, Int] ={

    var list: List[(String, Int)] = List[(String, Int)]()
    for (i <- 0 until e.size()){
      val obj: JSONObject = e.getObject(i, classOf[JSONObject])
      val tp: String = obj.getString("type")
      val tpv: List[(String, Int)] = tpSplit(tp)
      list ++= tpv
    }
    list.groupBy(_._1).mapValues(_.size)

  }


  def businessareaTags(rdd: RDD[JSONObject]): RDD[Map[(String, String), Int]] ={
    var list: List[((String, String), Int)] = List[((String, String), Int)]()
    val t: RDD[List[((String, String), Int)]] = rdd.map(e => {
      val arr: JSONArray = e.getObject("regeocode", classOf[JSONObject]).getObject("pois", classOf[JSONArray])
      for (i <- 0 until arr.size()){
        val nObject: JSONObject = arr.getObject(i, classOf[JSONObject])
        val id: String = nObject.getString("id")
        val businessarea: String = nObject.getString("businessarea")
        list :+= ((id, businessarea), 1)
//        println(list.mkString)
      }
      list
    })
    t.map(_.groupBy(e => {
      e._1
    }).mapValues(_.size))
//    println(list.size)
//    list
  }

  def typeTags(rdd: RDD[JSONObject]): RDD[List[(String, List[(String, Int)])]] ={
    var list: List[(String, List[(String, Int)])] = List[(String, List[(String, Int)])]()

    val t: RDD[List[(String, List[(String, Int)])]] = rdd.map(e => {
      val arr: JSONArray = e.getObject("regeocode", classOf[JSONObject]).getObject("pois", classOf[JSONArray])
      for (i <- 0 until arr.size()){
        val nObject: JSONObject = arr.getObject(i, classOf[JSONObject])
        val id: String = nObject.getString("id")
        val tp: String = nObject.getString("type")
        val lis: List[(String, Int)] = tpSplit(tp)
        list :+= (id, lis)
      }
      list
    })
//    println(list.size)
//    list
    t
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
