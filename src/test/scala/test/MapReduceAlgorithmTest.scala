package test
import mapreduce.MapReduceAlgorithms
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.immutable.HashMap

class MapReduceAlgorithmTest extends AnyFunSuite {

  test("Test Word Count"){
    val data=List((1,"Die Hochschule fuer Technik und Wirtschaft Berlin (HTW Berlin) ist mit fast 14.000 Studierenden "),
      (2,"und ueber 500 Beschaeftigten die groesste staatliche Fachhochschule Berlins und Ostdeutschlands."),
      (3,"Es existieren etwa 70 Studienangebote in den Bereichen Technik, Informatik, Wirtschaft, Kultur "),
      (4,"und Gestaltung. Die HTW Berlin verteilt sich auf zwei Standorte: den Campus Treskowallee "),
      (5,"in Berlin-Karlshorst und den Campus Wilhelminenhof in Berlin-Oberschoeneweide."))

    val erg=HashMap(("und" -> 5), ("studienangebote" -> 1), ("verteilt" -> 1), ("beschaeftigten" -> 1),
      ("hochschule" -> 1), ("berlins" -> 1), ("informatik" -> 1), ("wirtschaft" -> 2), ("zwei" -> 1), ("bereichen" -> 1),
      ("htw" -> 2), ("wilhelminenhof" -> 1), ("in" -> 3), ("etwa" -> 1), ("ist" -> 1), ("kultur" -> 1),
      ("karlshorst" -> 1), ("standorte" -> 1), ("berlin" -> 5), ("ueber" -> 1), ("groesste" -> 1),
      ("ostdeutschlands" -> 1), ("sich" -> 1), ("die" -> 3), ("fachhochschule" -> 1), ("treskowallee" -> 1), ("auf" -> 1),
      ("gestaltung" -> 1), ("staatliche" -> 1), ("studierenden" -> 1), ("es" -> 1), ("mit" -> 1), ("den" -> 3),
      ("oberschoeneweide" -> 1), ("fuer" -> 1), ("fast" -> 1), ("existieren" -> 1), ("campus" -> 2), ("technik" -> 2))
    val res= MapReduceAlgorithms.wordCount(data).toMap
    assert(res===erg)

  }

  test("Find all accessed resources"){

    val data:List[(Int,(String, String, String))]= List( (1,("10.01.2014","http://www.htw-berlin.de/index.html","123.45.212.122")),
      (2,("10.01.2014","http://www.htw-berlin.de/aktuelles.html","123.45.212.142")),
      (3,("10.01.2014","http://www.htw-berlin.de/aktuelles.html","123.34.12.132")),
      (4,("10.01.2014","http://www.htw-berlin.de/aktuelles.html","123.45.212.122")),
      (5,("10.01.2014","http://www.htw-berlin.de/forschung.html","111.35.22.32")),
      (6,("10.01.2014","http://www.htw-berlin.de/index.html","123.45.212.123")))

    val erg= List("http://www.htw-berlin.de/aktuelles.html","http://www.htw-berlin.de/forschung.html",
      "http://www.htw-berlin.de/index.html")

    val res= MapReduceAlgorithms.getAddresses(data).sorted
    assert(res===erg)
  }

  test("Count the number of accesses per day") {

    val data: List[(Int, (String, String, String))] = List((1, ("10.01.2014", "http://www.htw-berlin.de/index.html", "123.45.212.122")),
      (2, ("10.01.2014", "http://www.htw-berlin.de/aktuelles.html", "123.45.212.142")),
      (3, ("10.01.2014", "http://www.htw-berlin.de/aktuelles.html", "123.34.12.132")),
      (4, ("10.01.2014", "http://www.htw-berlin.de/aktuelles.html", "123.45.212.122")),
      (5, ("10.01.2014", "http://www.htw-berlin.de/forschung.html", "111.35.22.32")),
      (6, ("11.01.2014", "http://www.htw-berlin.de/index.html", "123.45.212.123")))

    val erg = Map("11.01.2014" -> 1, "10.01.2014" -> 5): Map[String, Int]
    val res = MapReduceAlgorithms.countAccessesPerDay(data).toMap
    assert(res === erg)
  }
  test("Count the number of accesses of each resource per day"){

    val data:List[(Int,(String, String, String))]= List( (1,("10.01.2014","http://www.htw-berlin.de/index.html","123.45.212.122")),
      (2,("10.01.2014","http://www.htw-berlin.de/aktuelles.html","123.45.212.142")),
      (3,("10.01.2014","http://www.htw-berlin.de/aktuelles.html","123.34.12.132")),
      (4,("10.01.2014","http://www.htw-berlin.de/aktuelles.html","123.45.212.122")),
      (5,("10.01.2014","http://www.htw-berlin.de/forschung.html","111.35.22.32")),
      (6,("10.01.2014","http://www.htw-berlin.de/index.html","123.45.212.123")))

    val erg = Map((("10.01.2014","http://www.htw-berlin.de/index.html") -> 2), (("10.01.2014","http://www.htw-berlin.de/aktuelles.html") -> 3),
      (("10.01.2014","http://www.htw-berlin.de/forschung.html") -> 1)): Map[(String, String), Int]
    val res = MapReduceAlgorithms.countResourceAccessesPerDay(data).toMap
    assert(res === erg)
  }

  test("Find Anagrams"){

    val data=List("otto","toto","hans","haus","heute","geist","huete","siegt","tuehe")
    val erg= Set(("otto",List("toto")), ("geist",List("siegt")), ("heute",List("huete", "tuehe")))
    val res= MapReduceAlgorithms.findAnagrams(data).toSet
    assert(res===erg)
  }

}
