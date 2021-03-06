package utils

import java.sql.{Connection, DriverManager}
import java.util

object ConnectPoolUtils {

  private val max = 50 // 连接池总数
  private val connectionNum = 10 // 每次连接数
  private val pool = new util.LinkedList[Connection]() // 连接池
  private var conNum = 0 // 当前连接数

  // 同步代码块

  def getConnections: Connection ={
    AnyRef.synchronized({
      if(pool.isEmpty) {
        // 加载驱动
        preGetConn()
        for(i <- 1 to connectionNum) {
          val co: Connection =
            DriverManager.getConnection(
            "jdbc:mysql://localhost:3306/dmp",
            "root",
            "1234"
          )
          pool.push(co)
          conNum += 1
        }
      }
      pool.poll()
    })
  }

  // 释放连接
  def releaseConn(co: Connection): Unit ={
    pool.push(co)
  }

  // 加载驱动
  def preGetConn(): Unit ={
    // 控制驱动
    if(conNum > max){
      println("no connection")
      Thread.sleep(2000)
      preGetConn()
    }else{
      Class.forName("com.mysql.jdbc.Driver")
    }
  }
}
