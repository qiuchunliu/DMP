package toHbase

import conf.ConfigManager
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object ReadHbase {

  def main(args: Array[String]): Unit = {

    val ssc: SparkSession = ConfigManager.fetchSparkSession()
    val tableName = "ns1:t2"

    /*
     * 以下两种方式都可以用来创建 conf
     */
    val conf: Configuration = ssc.sparkContext.hadoopConfiguration
//    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","t22")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set(TableInputFormat.INPUT_TABLE, tableName)

    val hdrdd: RDD[(ImmutableBytesWritable, Result)] = ssc.sparkContext.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],  // 要读取的数据的存储格式
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],  // 与“fClass”参数关联的键的“类”
      classOf[org.apache.hadoop.hbase.client.Result]  // 与“fClass”参数关联的值的“类”
    )

    hdrdd.foreach(e => {
      val result: Result = e._2
      val key: String = Bytes.toString(result.getRow)
      val value: String = Bytes.toString(result.getValue(Bytes.toBytes("fff"), Bytes.toBytes("tags")))
//      println(key, value)
      println(key, value)
    })


  }

}
