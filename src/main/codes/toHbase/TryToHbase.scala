package toHbase

import conf.ConfigManager
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object TryToHbase {

  def main(args: Array[String]): Unit = {
    val ssc: SparkSession = ConfigManager.fetchSparkSession()
    val conf: Configuration = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum","t22")
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    val tablename = "ns1:t1"
    conf.set(TableInputFormat.INPUT_TABLE, tablename)

    val hbRdd: RDD[(ImmutableBytesWritable, Result)] = ssc.sparkContext.newAPIHadoopRDD(
      conf,
      classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result]
    )

    val count: Long = hbRdd.count()
    println(count)
    hbRdd.foreach{case (_,result) =>
           //获取行键
           val key: String = Bytes.toString(result.getRow)
           //通过列族和列名获取列
           val name: String = Bytes.toString(result.getValue("f1".getBytes,"name".getBytes))
//           val age: Int = Bytes.toInt(result.getValue("f1".getBytes,"age".getBytes))
//           println("Row key:"+key+" Name:"+name+" Age:"+age)
      println(key, name)
         }


  }

}
