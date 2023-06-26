package test

import mapreduce.SimpleMapReduceFlatMap
import org.scalatest.funsuite.AnyFunSuite

import scala.collection.immutable.HashMap

class SimpleMapReduceTestFlatMap extends AnyFunSuite {

  test("Test Word Count"){
    val s="Die Hochschule fuer Technik und Wirtschaft Berlin (HTW Berlin) ist mit fast 14.000 Studierenden "+
      "und ueber 500 Beschaeftigten die groesste staatliche Fachhochschule Berlins und Ostdeutschlands."+
    "Es existieren etwa 70 Studienangebote in den Bereichen Technik, Informatik, Wirtschaft, Kultur "+
      "und Gestaltung. Die HTW Berlin verteilt sich auf zwei Standorte: den Campus Treskowallee "+
      "in Berlin-Karlshorst und den Campus Wilhelminenhof in Berlin-Oberschoeneweide."

    val erg=HashMap(("und" -> 5), ("studienangebote" -> 1), ("verteilt" -> 1), ("beschaeftigten" -> 1),
      ("hochschule" -> 1), ("berlins" -> 1), ("informatik" -> 1), ("wirtschaft" -> 2), ("zwei" -> 1), ("bereichen" -> 1),
      ("htw" -> 2), ("wilhelminenhof" -> 1), ("in" -> 3), ("etwa" -> 1), ("ist" -> 1), ("kultur" -> 1),
      ("karlshorst" -> 1), ("standorte" -> 1), ("berlin" -> 5), ("ueber" -> 1), ("groesste" -> 1),
      ("ostdeutschlands" -> 1), ("sich" -> 1), ("die" -> 3), ("fachhochschule" -> 1), ("treskowallee" -> 1), ("auf" -> 1),
      ("gestaltung" -> 1), ("staatliche" -> 1), ("studierenden" -> 1), ("es" -> 1), ("mit" -> 1), ("den" -> 3),
        ("oberschoeneweide" -> 1), ("fuer" -> 1), ("fast" -> 1), ("existieren" -> 1), ("campus" -> 2), ("technik" -> 2))
    val res= SimpleMapReduceFlatMap.wordCount(s)
    assert(res===erg)

  }

  test("Count Resource Accesses") {

    val data: List[(String, String, String)] = List(("10.01.2014", "http://www.htw-berlin.de/index.html", "123.45.212.122"),
      ("10.01.2014", "http://www.htw-berlin.de/aktuelles.html", "123.45.212.142"), ("10.01.2014", "http://www.htw-berlin.de/aktuelles.html", "123.34.12.132"),
      ("10.01.2014", "http://www.htw-berlin.de/aktuelles.html", "123.45.212.122"), ("10.01.2014", "http://www.htw-berlin.de/forschung.html", "111.35.22.32"),
      ("10.01.2014", "http://www.htw-berlin.de/index.html", "123.45.212.123"))
    val erg = Map((("10.01.2014","http://www.htw-berlin.de/index.html") -> 2), (("10.01.2014","http://www.htw-berlin.de/aktuelles.html") -> 3),
      (("10.01.2014","http://www.htw-berlin.de/forschung.html") -> 1)): Map[(String, String), Int]
    val res = SimpleMapReduceFlatMap.countResourceAccessesPerDay(data)
    assert(res === erg)
  }

  test("Count Letters"){

    val data= List("Dies", "ist", "ein", "Text", "und", "das", "ist", "noch", "ein", "Text")
    val erg= Map('e' -> 5, 's' -> 4, 'x' -> 2, 'n' -> 4, 't' -> 6, 'u' -> 1, 'a' -> 1, 'i' -> 5, 'c' -> 1, 'h' -> 1, 'o' -> 1, 'd' -> 3)
    val res = SimpleMapReduceFlatMap.countLetters(data)
    assert(res === erg)
  }

  test("Prime divider Function") {

    val erg = List(2, 2, 2, 3)
    val res = SimpleMapReduceFlatMap.primteiler(24)
    assert(res === erg)
  }

  test("Count Prime Divider") {

    val data = List(24,12,9,10)
    val erg = Map(2->6,3->4,5->1)
    val res = SimpleMapReduceFlatMap.countPrimeDivider(data)
    assert(res === erg)
  }
}
