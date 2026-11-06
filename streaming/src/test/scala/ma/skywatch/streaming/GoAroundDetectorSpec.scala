package ma.skywatch.streaming

import java.sql.Timestamp

import org.scalatest.funsuite.AnyFunSuite

class GoAroundDetectorSpec extends AnyFunSuite {

  private val T0 = 1794000000L

  // à environ 2 km des pistes de Tanger (GMTT, 21 m) et de Fès (GMFF, 579 m)
  private val Tanger = (35.74, -5.93)
  private val Fes    = (33.94, -4.99)

  /** Un avion qui suit un profil d'altitudes, une position toutes les 45 s.
    * None = position au sol (on_ground = true, pas d'altitude).
    */
  private def profile(alts: Seq[Option[Double]], where: (Double, Double) = Tanger, from: Int = 0): Seq[Position] =
    alts.zipWithIndex.map { case (alt, k) =>
      val t = T0 + (from + k) * 45L
      Position("fff002", Some("DEMO02"), t, new Timestamp(t * 1000), where._1, where._2, alt, Some(280.0), alt.isEmpty)
    }

  private def air(alts: Double*): Seq[Option[Double]] = alts.map(Some(_))

  test("descente vers Tanger jusqu'à 300 m puis remontée : alerte GO_AROUND") {
    val ga = profile(air(1500, 1200, 900, 600, 300, 600, 900))
    val (_, alert) = GoAroundDetector.update(GoAroundState.Empty, ga)
    assert(alert.isDefined)
    val a = alert.get
    assert(a.alertType == "GO_AROUND")
    assert(a.airport == "GMTT")
    assert(a.lowestAltitudeM == 300.0)
    assert(a.lowestAglM == 279.0) // 300 m - 21 m (altitude de la piste)
    assert(a.climbM == 600.0)
    assert(a.lowestAt == T0 + 4 * 45)
  }

  test("un atterrissage normal (l'avion touche le sol) ne déclenche rien") {
    val landing = profile(air(1200, 900, 600, 300) ++ Seq(None, None))
    assert(GoAroundDetector.update(GoAroundState.Empty, landing)._2.isEmpty)
  }

  test("un avion encore en descente (pas de remontée) ne déclenche rien") {
    assert(GoAroundDetector.update(GoAroundState.Empty, profile(air(1500, 1200, 900, 600, 300)))._2.isEmpty)
  }

  test("un décollage (montée sans descente avant) ne déclenche rien") {
    assert(GoAroundDetector.update(GoAroundState.Empty, profile(air(100, 400, 800, 1200)))._2.isEmpty)
  }

  test("un creux en altitude (3 000 m) n'est pas une remise de gaz") {
    assert(GoAroundDetector.update(GoAroundState.Empty, profile(air(3500, 3200, 3000, 3400, 3800)))._2.isEmpty)
  }

  test("loin de tout aéroport, rien") {
    val atlantique = profile(air(1500, 1200, 900, 600, 300, 600, 900), where = (30.603, -12.3228))
    assert(GoAroundDetector.update(GoAroundState.Empty, atlantique)._2.isEmpty)
  }

  test("l'altitude de la piste compte : 900 m à Fès = 321 m au-dessus de la piste, alerte") {
    val fes = profile(air(1800, 1500, 1200, 900, 1200, 1500), where = Fes)
    val (_, alert) = GoAroundDetector.update(GoAroundState.Empty, fes)
    assert(alert.map(_.airport).contains("GMFF"))
    assert(alert.get.lowestAglM == 321.0)
  }

  test("le même profil à Tanger (879 m au-dessus de la piste) ne déclenche rien") {
    val tanger = profile(air(1800, 1500, 1200, 900, 1200, 1500), where = Tanger)
    assert(GoAroundDetector.update(GoAroundState.Empty, tanger)._2.isEmpty)
  }

  test("la descente et la remontée peuvent arriver dans deux batchs différents") {
    val (s1, a1) = GoAroundDetector.update(GoAroundState.Empty, profile(air(1500, 1200, 900, 600, 300)))
    assert(a1.isEmpty)
    val (_, a2) = GoAroundDetector.update(s1, profile(air(600, 900), from = 5))
    assert(a2.isDefined)
  }

  test("une seule alerte toutes les 15 minutes pour le même avion") {
    val (s1, a1) = GoAroundDetector.update(GoAroundState.Empty, profile(air(1500, 1200, 900, 600, 300, 600, 900)))
    assert(a1.isDefined)
    val (_, a2) = GoAroundDetector.update(s1, profile(air(1200, 1500), from = 7)) // il continue de monter
    assert(a2.isEmpty)
  }

  test("les positions de plus de 5 minutes sont oubliées") {
    val (s1, _) = GoAroundDetector.update(GoAroundState.Empty, profile(air(1500, 1200, 900)))
    val (s2, _) = GoAroundDetector.update(s1, profile(air(2000), from = 20)) // 15 minutes plus tard
    assert(s2.positions.size == 1)
  }
}