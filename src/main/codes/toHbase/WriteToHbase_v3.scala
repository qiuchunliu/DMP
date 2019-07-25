package toHbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import tags.TagsMain


/**
  * 使用saveAsNewAPIHadoopDataset写入数据
  */
object WriteToHbase_v3 {

  def main(args: Array[String]): Unit = {

    val tags: RDD[(String, List[(String, Int)])] = TagsMain.fetchTags()
    val tablename = "ns1:t2"

    val conf: Configuration = HBaseConfiguration.create()
    // 设置zookeeper
    conf.set("hbase.zookeeper.quorum","t22")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    // 设置输出表
    // 要用这个包，不要导错包
    // import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
    conf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    val job = new Job(conf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    tags.map(e => {
      val put = new Put(e._1.getBytes)
      put.addColumn("fff".getBytes, "tags".getBytes, e._2.mkString(",").getBytes)
      (new ImmutableBytesWritable, put)
    }).saveAsNewAPIHadoopDataset(job.getConfiguration)
    // 可以存入hbase 但是报错
  }

}
