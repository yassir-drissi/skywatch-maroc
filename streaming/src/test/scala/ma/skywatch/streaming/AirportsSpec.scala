package ma.skywatch.streaming

import org.scalatest.funsuite.AnyFunSuite

class AirportsSpec extends AnyFunSuite {

  test("un avion à 14 km de Casablanca est dans la zone de GMMN") {
    assert(Airports.within(33.45, -7.48, Airports.TerminalAreaKm).map(_.icao).contains("GMMN"))
  }

  test("entre Casablanca et Rabat, l'aéroport le plus proche gagne") {
    // 22 km de Rabat-Salé, 87 km de Casablanca
    assert(Airports.within(33.9, -6.9, Airports.TerminalAreaKm).map(_.icao).contains("GMME"))
  }

  test("un avion au-dessus de l'Atlantique n'est dans aucune zone") {
    // survol à 280 km d'Agadir
    assert(Airports.within(30.603, -12.3228, Airports.TerminalAreaKm).isEmpty)
  }

  test("la limite du rayon est respectée") {
    // 13,7 km de GMMN : dedans avec 20 km, dehors avec 10 km
    assert(Airports.within(33.45, -7.48, 20).isDefined)
    assert(Airports.within(33.45, -7.48, 10).isEmpty)
  }
}