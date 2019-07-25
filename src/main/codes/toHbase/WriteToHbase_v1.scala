package toHbase

import conf.ConfigManager
import indicator.CountIndicators
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put, Table}
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.spark.rdd.RDD
import tags.TagsMain


/**
  * 将数据写入hbase
  * 方式一：简单粗暴版
  */
object WriteToHbase_v1 {

  def main(args: Array[String]): Unit = {

    val tags: RDD[(String, List[(String, Int)])] = TagsMain.fetchTags()
    val tablename = "ns1:t2"
    tags.foreachPartition(p => {
      // 建立hbase连接
      val conf: Configuration = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum","t22")
      conf.set("hbase.zookeeper.property.clientPort", "2181")
      conf.set(TableInputFormat.INPUT_TABLE, tablename)

      val conn: Connection = ConnectionFactory.createConnection(conf)
      val table: Table = conn.getTable(TableName.valueOf(tablename))
      p.foreach(e => {
        val put = new Put(e._1.getBytes)
        put.addColumn("f".getBytes(), "tags".getBytes(), e._2.toString().getBytes)
        table.put(put)  // Puts some data in the table.
      })
      table.close()
      conn.close()
    })

  }

}
