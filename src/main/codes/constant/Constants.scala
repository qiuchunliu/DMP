package constant

object Constants {

  val MASTER = "local[2]"
  val SERIALIZABLE = "org.apache.spark.serializer.KryoSerializer"
  val COMPRESSION = "snappy"
  val JDBC_URL = "jdbc:mysql://localhost:3306/dmp"
  val JDBC_USER = "root"
  val JDBC_PASSWORD = "1234"

}
