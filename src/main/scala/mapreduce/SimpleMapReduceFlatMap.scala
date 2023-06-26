package mapreduce

object SimpleMapReduceFlatMap {

  def mapReduce[S,B,R](mapFun:(S=>List[B]), redFun:(R,B)=>R,
                       base:R, l:List[S]):R =
    (l.flatMap(mapFun)).foldLeft(base)(redFun)

  /*
    Change the function wordCount from SimpleMapReduce that it works with the
    flatMap-Variant of MapReduce
   */

  def wordCount(text:String):Map[String,Int]={

    val t= text.toLowerCase.replaceAll("[^a-z]", " ")
    val wl= t.split(" ").filter(_!="").toList

    mapReduce[String, (String, Int), Map[String, Int]](
      (x:String)=>List((x,1)),
      (m:Map[String,Int],e:(String,Int))=> m.updated(e._1, m.getOrElse(e._1,0)+e._2),
      Map():Map[String,Int],
      wl)
  }

  /*
      Change the function countResourceAccessesPerDay from SimpleMapReduce that it works with the
      flatMap-Variant of MapReduce
     */

   def countResourceAccessesPerDay(data:List[(String,String,String)]): Map[(String,String),Int] = {
     mapReduce[(String, String, String), (String, String), Map[(String, String), Int]](
       (x: (String, String, String)) => List((x._1, x._2)),
       (m: Map[(String, String), Int], e: (String, String)) => m.updated(e, m.getOrElse(e, 0) + 1),
       Map(): Map[(String, String), Int],
       data)
   }

  /*
    Write a functions that simply counts letters from a list of words.
    Use mapReduce utilizing the flatMap-Function
   */
  def countLetters(l: List[String]): Map[Char, Int] = {
    mapReduce[String, (Char, Int), Map[Char, Int]](
      x => x.toLowerCase.toSeq.map((_, 1)).toList,
      (map, el) => map.updated(el._1, map.getOrElse(el._1, 0) + ???),
      Map(): Map[Char, Int],
      l)
  }

  /*
    Write a function that count the prime dividers of a list
    of integers
    Use the mapReduce for counting and the primteiler function to calculate
    the prime dividers of one number
   */
  def primteiler(z: Int): List[Int] = {

    def prim(x: Int, teiler: Int): List[Int] = x match {

      case 1 => List()
      case _ if (x % teiler == 0) => teiler :: prim(x / teiler, teiler)
      case _ => prim(x, teiler + 1)
    }

    prim(z, 2)
  }

  def countPrimeDivider(l: List[Int]): Map[Int, Int] = ???

}
