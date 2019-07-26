package utils

import com.alibaba.fastjson.{JSON, JSONObject}
import redis.clients.jedis.Jedis
import scala.util.matching.Regex

object parseJson {

  def main(args: Array[String]): Unit = {

    val jsons: String =
      """
        |{"name":"桔子桑",
        |  "sex":"男",
        |  "age":18,
        |"grade":{"gname":"三年八班",
        |         "gdesc":"初三年级八班"
        |         }
        | }
      """.stripMargin

//    val s = "{'name':'桔子桑'}"
//    val jobj: JSONObject = JSON.parseObject(s)
//    val jso: String = jobj.getString("grade")
//    println(JSON.parseObject(jso).getString("gdesc"))




//    val jedis = new Jedis("t21", 6379)
//
    val s = "dertyfdghjrtyfd2fjtyjefdawetrf"

    val re = new Regex("d(.*?)f")
    println(re.findAllIn(s).toArray.mkString(","))


    val regex = new Regex(""""name":"(.*?)",""") // dertyf,dghjrtyf,d2f,dawetrf

    println(
      regex
        .findAllIn(jsons)
//        .group(1)
        .toArray
        .map(e => {
      e.substring(8, e.indexOf("""","""))
    })
        .mkString(","))


    println(regex.findAllIn(s).toArray.mkString(","))

  }

}
