package ma.skywatch.streaming

import org.scalatest.funsuite.AnyFunSuite

class GeoSpec extends AnyFunSuite {

  private val Tolerance = 0.5 // km

  test("un degré de longitude à l'équateur fait environ 111 km") {
    assert(math.abs(Geo.haversineKm(0, 0, 0, 1) - 111.19) < Tolerance)
  }

  test("distance Casablanca (GMMN) - Rabat (GMME)") {
    val d = Geo.haversineKm(33.3675, -7.5900, 34.0515, -6.7515)
    assert(math.abs(d - 108.63) < Tolerance)
  }

  test("la distance d'un point à lui-même est nulle") {
    assert(Geo.haversineKm(33.0, -7.0, 33.0, -7.0) == 0.0)
  }

  test("virage à droite en passant par le nord : 350° -> 10° = +20°") {
    assert(Geo.headingDelta(350, 10) == 20)
  }

  test("virage à gauche en passant par le nord : 10° -> 350° = -20°") {
    assert(Geo.headingDelta(10, 350) == -20)
  }

  test("petit virage sans passer par le nord") {
    assert(Geo.headingDelta(90, 135) == 45)
    assert(Geo.headingDelta(135, 90) == -45)
  }

  test("demi-tour : le résultat reste dans [-180, 180]") {
    val d = Geo.headingDelta(0, 180)
    assert(d >= -180 && d <= 180 && math.abs(d) == 180)
  }

  test("l'aéroport le plus proche d'un avion près de Casablanca est GMMN") {
    val (airport, km) = Airports.nearest(33.45, -7.48)
    assert(airport.icao == "GMMN")
    assert(km < 20)
  }

  test("les 6 aéroports ont des codes OACI marocains") {
    assert(Airports.All.map(_.icao).toSet == Set("GMMN", "GMMX", "GMME", "GMFF", "GMTT", "GMAD"))
    assert(Airports.All.forall(_.icao.startsWith("GM")))
  }
}