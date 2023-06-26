package mapreduce

object SimpleMapReduce {

  def mapReduce[S,B,R](mapFun:(S=>B), redFun:(R,B)=>R,
                       base:R, l:List[S]):R =
    (l.map(mapFun)).foldLeft(base)(redFun)

  /*
    Function that implements the wordcount algorithm
    Step 1: Preprocessing - extracting words
    Step 2: Bound every word to a value of occurences
    Step 3: Group all the occurence-values and sum them up
   */
  def wordCount(text:String):Map[String,Int]={

    val t= text.toLowerCase.replaceAll("[^a-z]", " ")
    val wl= t.split(" ").filter(_!="").toList

    mapReduce[String, (String, Int), Map[String, Int]](
      (x:String)=>(x,1),
      (m:Map[String,Int],e:(String,Int))=> m.updated(e._1, m.getOrElse(e._1,0)+e._2),
      Map():Map[String,Int],
      wl)
  }

  /*
    Given is some data of an apache log file containing:
    (date,resource,requesting ip-address)

    Write a function that uses only mapreduce that counts
    all resource accesses per day
    use a similar approach

    Result should be a Map that associates the combination of day and resource
    with the number of accesses
   */

   def countResourceAccessesPerDay(data:List[(String,String,String)]): Map[(String,String),Int] = {

     mapReduce[(String, String, String), (String, String), Map[(String, String), Int]](
       (x:(String, String, String)) => (x._1, x._2),
       (m: Map[(String, String), Int], e: (String, String)) => m.updated(e, m.getOrElse(e, 0) + 1),
       Map():Map[(String, String),Int],
       data)
   }

  /*
      Given is some data of an apache log file containing:
      (date,resource,requesting ip-address)

      Write a function that uses only mapreduce that counts
      all resource accesses per IP address

      Result should be a Map that associates the IP with
      the number of accesses
     */
  def countResourceAccessesPerIP(data: List[(String, String, String)]): Map[String, Int] = {
    mapReduce[(String, String, String), (String, Int), Map[String, Int]](
      x => (x._3, 1),
      (m: Map[String, Int], e: (String, Int)) => m.updated(e._1, m.getOrElse(e._1, 0) + e._2),
      Map(): Map[String, Int],
      data)
  }
}
