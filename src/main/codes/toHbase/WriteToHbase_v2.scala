package toHbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.rdd.RDD
import tags.TagsMain


/**
  * 使用saveAsHadoopDataset写入数据
  */
object WriteToHbase_v2 {

  def main(args: Array[String]): Unit = {

    val tags: RDD[(String, List[(String, Int)])] = TagsMain.fetchTags()
    val tablename = "ns1:t2"

    val conf: Configuration = HBaseConfiguration.create()
    // 设置zookeeper
    conf.set("hbase.zookeeper.quorum","t22")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    // Construct a map/reduce job configuration.
    val jobConf: JobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    /*
     * 一个Put对象就是一行记录，在构造方法中指定主键
     * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
     * Put.add方法接收三个参数：列族，列名，数据
     */
    tags.map(p => {
      val put = new Put(p._1.getBytes)
        put.addColumn("ff".getBytes, "tags".getBytes, p._2.toString.getBytes)
        (new ImmutableBytesWritable, put)
      }).saveAsHadoopDataset(jobConf)

  }

}
