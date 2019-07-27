object test {

  def main(args: Array[String]): Unit = {
//    f("23")

    val arr = Array(1,2,3,4)
    val lis: List[Int] = arr.toList

    val ls: List[(Int, Int)] = arr.map(e => (e, 1)).toList
    val res: Int = ls.foldLeft(0)(_ + _._1)
    println(res)
  }

  /**
    * s = null 表示如果不传值，则不会在内存中开辟空间
    * @param s 如果 s = "" 表示s 默认是空串，在内存中开辟了空间
    */
  def f(s: String = null): Unit ={

    println(s)

  }

}
