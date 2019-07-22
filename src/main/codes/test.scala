import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object test {

  val sk: SparkSession =
    SparkSession.builder()
      .master("local[2]")
      .appName(this.getClass.getName)
      .getOrCreate()
  sk.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
  sk.sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

  def main(args: Array[String]): Unit = {
    print()
  }

  def runIt(): Unit ={
    val lines: RDD[String] = sk.sparkContext.textFile("D:\\programs\\java_id" +
      "ea\\DMP\\src\\files\\2016-10-" +
      "01_06_p1_invalid.1475274123982.log")
    val arrRdd: RDD[Array[String]] = lines.map(e => {e.split(",", -1)}).filter(_.length >= 85)
    utils.UtilsForProj(arrRdd)  // 创建 Row的RDD
  }

}
// df.write.partitionBy().