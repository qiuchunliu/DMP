package conf

import java.util.Properties

import constant.Constants
import org.apache.spark.sql.SparkSession

object ConfigManager {

  /* 获取sparkSession*/
  def fetchSparkSession(): SparkSession ={
    SparkSession.builder()
        .master(Constants.MASTER)
        .appName(this.getClass.getName)
        .config("spark.serializer", Constants.SERIALIZABLE)
        .config("spark.sql.parquet.compression.codec", Constants.COMPRESSION)
        .getOrCreate()
  }

  /* 获取jdbc连接 */
  def fetchJDBC(): (String, Properties) ={
    val url: String = Constants.JDBC_URL
    val properties = new Properties()
    properties.put("user", Constants.JDBC_USER)
    properties.put("password", Constants.JDBC_PASSWORD)
    (url, properties)
  }

}
