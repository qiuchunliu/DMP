import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import utils.UtilsForProj

object test {

  val sk: SparkSession =
    SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getName)
      .getOrCreate()
  sk.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  sk.sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

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
    df.createTempView("vv")
    df.sqlContext.sql("select sessionid from vv limit 3").show()
  }

}
// df.write.partitionBy().