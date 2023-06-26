package mapreduce

import mapreduce.BasicOperations.mapReduce

object MapReduceAlgorithms {

  /*
  	 * Wandeln Sie das WordCount-Beispiel aus der Vorlesung in die Map-Reduce-Variante um.
  	 * Die Funktion soll wie unten aufgefuehrt aufgerufen werden koennen.
  	 *
  	 * */
  def wordCount(text:List[(Int,String)]):List[(String,Int)] = {

    mapReduce[Int, String, String, Int, String, Int](
      (X)=>{
        val t= X._2.toLowerCase.replaceAll("[^a-z]", " ")
        t.split(" ").toList.map(X=>(X,1)).filter(X=>X._1!="")
      },
      (X) => List((X._1, X._2.sum)),
      text)
  }
  /*
     * Schreiben Sie eine Funktion, die fuer die Liste von WebLog-Einträgen ermittelt, welche
     * Adressen (Ressourcen) aufgerufen worden sind.
     */

  def getAddresses(data:List[(Int,(String, String, String))]):List[String]= {
      mapReduce[Int, (String, String, String), String, Int, List[String], Int](
        kv => List((kv._2._2, -1)),
        kv => List((List(kv._1), -1)).map(_._1)
      )
  }

  /*
 * Schreiben Sie eine Funktion, die fuer die Liste von WebLog-Einträgen ermittelt, an welchen Tagen
 * wie haeufig zugegriffen wurde.
 */
  def countAccessesPerDay(data: List[(Int, (String, String, String))]): List[(String, Int)] = ???
  /*
 * Schreiben Sie eine Funktion, die fuer die Liste von WebLog-Einträgen ermittelt, welche
 * Adressen (Ressourcen) an welchem Tag wie haeufig aufgerufen worden sind.
 */
  def countResourceAccessesPerDay(data:List[(Int,(String, String, String))]):List[((String,String),Int)]= ???

  /* Schreiben Sie eine Funktion, die für eine Liste von Wörtern alle Anagramme findet
   * Anagramme sind Wörter, die dieselben Buchstaben haben.
     * Benutzen Sie dafür die MapReduce-Funktion
     * Das Ergebnis soll folgendermaßen aussehen:
     * Wenn für ein Wort keine Anagramme gefunden wurden, dann soll das Wort auch nicht in der Ergebnisliste erscheinen
     * Wenn ein oder mehrere Anagramme gefunden wurden, dann soll ein Ergebnistupel erzeugt werden, dass das
     * lexikalisch kleinste Element als Schlüssel beinhaltet und der Wert soll dann eine Liste aller gefundenen Anagramme
     * (ohne das erste Wort) sein in alphabetischer Reihenfolge sortiert (siehe Test).
     */

  def findAnagrams(l: List[String]): List[(String, List[String])] = ???
}
