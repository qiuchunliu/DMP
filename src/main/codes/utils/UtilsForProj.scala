package utils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object UtilsForProj {

  def makeRow(arrRdd: RDD[Array[String]]): Unit ={
    arrRdd.map(arr => {
      Row(
        arr(0)
      )
    })
  }

}
