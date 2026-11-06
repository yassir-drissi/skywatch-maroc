package ma.skywatch.streaming

import java.sql.Timestamp

import org.scalatest.funsuite.AnyFunSuite

class HoldingDetectorSpec extends AnyFunSuite {

  private val T0 = 1794000000L // instant de départ des trajectoires de test

  // près de Tanger (GMTT), à environ 15 km de la piste
  private val TangerLat = 35.80
  private val TangerLon = -5.80

  /** Une position n°i, toutes les `step` secondes. */
  private def pos(
      i: Int,
      track: Double,
      step: Long = 45,
      alt: Double = 2500,
      lat: Double = TangerLat,
      lon: Double = TangerLon,
      onGround: Boolean = false
  ): Position = {
    val t = T0 + i * step
    Position("fff001", Some("DEMO01"), t, new Timestamp(t * 1000), lat, lon, Some(alt), Some(track), onGround)
  }

  /** Un avion qui tourne en rond : +90° toutes les 45 s (un tour en 3 minutes). */
  private def circling(n: Int, from: Int = 0, alt: Double = 2500, lat: Double = TangerLat, lon: Double = TangerLon) =
    (from until from + n).map(i => pos(i, (i * 90) % 360, alt = alt, lat = lat, lon = lon))

  test("somme des virages, avec le passage par le nord (270° -> 0° = +90°)") {
    val tracks = Seq(0.0, 90.0, 180.0, 270.0, 0.0)
    val positions = tracks.zipWithIndex.map { case (t, i) => pos(i, t) }
    assert(HoldingDetector.totalTurn(positions) == 360.0)
  }

  test("un avion qui fait deux tours près de Tanger déclenche une alerte HOLDING") {
    val (_, alert) = HoldingDetector.update(HoldingState.Empty, circling(8)) // 7 virages de 90° = 630°
    assert(alert.isDefined)
    val a = alert.get
    assert(a.alertType == "HOLDING")
    assert(a.airport == "GMTT")
    assert(a.totalTurnDeg == 630.0)
    assert(a.windowStart == T0)
    assert(a.detectedAt == T0 + 7 * 45)
    assert(a.positions == 8)
  }

  test("un vol en ligne droite ne déclenche rien") {
    val straight = (0 until 10).map(i => pos(i, 45.0))
    assert(HoldingDetector.update(HoldingState.Empty, straight)._2.isEmpty)
  }

  test("des zigzags gauche-droite s'annulent : pas d'alerte") {
    val zigzag = (0 until 12).map(i => pos(i, if (i % 2 == 0) 0.0 else 60.0))
    assert(HoldingDetector.update(HoldingState.Empty, zigzag)._2.isEmpty)
  }

  test("un avion qui tourne en altitude de croisière (11 000 m) ne déclenche rien") {
    assert(HoldingDetector.update(HoldingState.Empty, circling(8, alt = 11000))._2.isEmpty)
  }

  test("un avion qui tourne loin de tout aéroport ne déclenche rien") {
    val atlantique = circling(8, lat = 30.603, lon = -12.3228) // 280 km d'Agadir
    assert(HoldingDetector.update(HoldingState.Empty, atlantique)._2.isEmpty)
  }

  test("un avion au sol ne déclenche rien (roulage)") {
    val taxi = (0 until 8).map(i => pos(i, (i * 90) % 360, onGround = true))
    assert(HoldingDetector.update(HoldingState.Empty, taxi)._2.isEmpty)
  }

  test("si les positions sont trop espacées (2 min), les virages ne sont pas comptés") {
    val sparse = (0 until 8).map(i => pos(i, (i * 90) % 360, step = 120))
    assert(HoldingDetector.totalTurn(sparse) == 0.0)
    assert(HoldingDetector.update(HoldingState.Empty, sparse)._2.isEmpty)
  }

  test("l'état garde les positions d'un batch à l'autre") {
    val (s1, a1) = HoldingDetector.update(HoldingState.Empty, circling(3)) // 180° seulement
    assert(a1.isEmpty)
    assert(s1.positions.size == 3)
    val (_, a2) = HoldingDetector.update(s1, circling(5, from = 3)) // on continue le même tour
    assert(a2.isDefined)
    assert(a2.get.totalTurnDeg == 630.0)
  }

  test("une seule alerte toutes les 15 minutes pour le même avion") {
    val (s1, a1) = HoldingDetector.update(HoldingState.Empty, circling(8))
    assert(a1.isDefined)
    assert(s1.lastAlertTime == T0 + 7 * 45)
    val (_, a2) = HoldingDetector.update(s1, circling(4, from = 8)) // il continue de tourner 3 min plus tard
    assert(a2.isEmpty)
  }

  test("les positions de plus de 10 minutes sont oubliées") {
    val (s1, _) = HoldingDetector.update(HoldingState.Empty, circling(8))
    val (s2, _) = HoldingDetector.update(s1, Seq(pos(30, 0.0))) // 17 minutes plus tard
    assert(s2.positions.map(_.time) == Seq(T0 + 30 * 45))
  }

  test("une position reçue deux fois n'est comptée qu'une fois") {
    val twice = circling(3) ++ circling(3)
    val (s, _) = HoldingDetector.update(HoldingState.Empty, twice)
    assert(s.positions.size == 3)
  }
}