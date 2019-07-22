import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import utils.UtilsForProj

object test {

  val sk: SparkSession =
    SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getName)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.parquet.compression.codec", "snappy")
      .getOrCreate()

  def main(args: Array[String]): Unit = {
    runIt()
  }

  def runIt(): Unit ={
    val lines: RDD[String] = sk.sparkContext.textFile("D:\\programs\\java_id" +
      "ea\\DMP\\src\\files\\2016-10-" +
      "01_06_p1_invalid.1475274123982.log")
    val arrRdd: RDD[Array[String]] = lines.map(e => {e.split(",", -1)}).filter(_.length >= 85)
    val rowRdd: RDD[Row] = UtilsForProj.makeRow(arrRdd)  // 创建 Row 的 RDD
    val struct: StructType = UtilsForProj.makeStructure()
    val df: DataFrame = sk.createDataFrame(rowRdd, struct)
//    df.createTempView("vv")
//    df.sqlContext.sql("select sessionid from vv limit 3").show()  // 可查
//    df.write.parquet("D:\\programs\\java_idea\\DMP\\src\\outPutFiles\\out")  // 写出到parquet文件
    import sk.implicits._
    val ct_prov_city_df: DataFrame = df.rdd.map(e => {
      ((e.getAs[String]("provincename"),
      e.getAs[String]("cityname")), 1)
    })
      .reduceByKey(_+_)  // 按照省市进行统计
      .map(t => (t._2, t._1._1, t._1._2))  // 返回一个 (省，市，个数)的rdd
      .toDF("ct", "provincename", "cityname")

    /* 把数据存入mysql */
//    UtilsForProj.loadToMysql(ct_prov_city_df)
    /* 以json形式存入文件 */
    UtilsForProj.loadToJsonFile(ct_prov_city_df)

  }

}
// df.write.partitionBy().