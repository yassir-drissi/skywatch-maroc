package ma.skywatch.streaming

import java.sql.Timestamp

/** Une position d'avion, avec seulement les champs utiles à la détection. */
final case class Position(
    icao24: String,
    callsign: Option[String],
    time: Long,           // snapshot_time, en secondes Unix
    eventTime: Timestamp, // le même instant en Timestamp (porte le watermark de Spark)
    lat: Double,
    lon: Double,
    altitudeM: Option[Double],
    track: Option[Double], // cap en degrés, 0 = nord
    onGround: Boolean
)

/** Ce que Spark garde en mémoire pour CHAQUE avion entre deux micro-batchs. */
final case class HoldingState(positions: Seq[Position], lastAlertTime: Long)

object HoldingState {
  val Empty: HoldingState = HoldingState(Seq.empty, 0L)
}

/** Une alerte envoyée dans le topic flights.alerts. */
final case class HoldingAlert(
    alertType: String,
    icao24: String,
    callsign: Option[String],
    airport: String,
    detectedAt: Long,   // secondes Unix de la dernière position
    windowStart: Long,  // secondes Unix de la première position prise en compte
    totalTurnDeg: Double,
    altitudeM: Option[Double],
    lat: Double,
    lon: Double,
    positions: Int
)

/** Détection des attentes en circuit, en Scala pur (sans Spark) pour pouvoir la tester.
  *
  * Règle : sur les 10 dernières minutes, l'avion a tourné d'au moins 360° dans le même
  * sens, il vole sous 6 000 m et il est à moins de 60 km d'un aéroport.
  */
object HoldingDetector {

  val WindowS: Long     = 600    // on regarde les 10 dernières minutes
  val MinTurnDeg        = 360.0  // un tour complet
  val MaxAltitudeM      = 6000.0 // les circuits d'attente sont en général entre 1 500 et 4 500 m
  val MaxGapS: Long     = 60     // au-delà, il manque des positions : on ne compte pas ce virage
  val CooldownS: Long   = 900    // au plus une alerte toutes les 15 minutes par avion

  /** Somme des virages entre positions consécutives (droite = +, gauche = -).
    *
    * Si deux positions sont trop éloignées dans le temps, on ne sait pas ce que l'avion a fait
    * entre les deux : ce virage n'est pas compté.
    */
  def totalTurn(positions: Seq[Position]): Double =
    positions
      .sliding(2)
      .collect {
        case Seq(a, b) if b.time - a.time <= MaxGapS && a.track.isDefined && b.track.isDefined =>
          Geo.headingDelta(a.track.get, b.track.get)
      }
      .sum

  /** Ajoute les nouvelles positions à l'état de l'avion et décide s'il faut une alerte. */
  def update(state: HoldingState, batch: Seq[Position]): (HoldingState, Option[HoldingAlert]) = {
    // anciennes + nouvelles positions, une seule par instantané, dans l'ordre du temps
    val merged = (state.positions ++ batch).groupBy(_.time).values.map(_.head).toSeq.sortBy(_.time)

    if (merged.isEmpty) (state, None)
    else {
      val latest     = merged.last
      val recent     = merged.filter(_.time > latest.time - WindowS) // les 10 dernières minutes
      val turn       = totalTurn(recent)
      val low        = !latest.onGround && latest.altitudeM.exists(_ <= MaxAltitudeM)
      val cooledDown = latest.time - state.lastAlertTime >= CooldownS

      val alert = Airports
        .within(latest.lat, latest.lon, Airports.TerminalAreaKm)
        .filter(_ => math.abs(turn) >= MinTurnDeg && low && cooledDown)
        .map { airport =>
          HoldingAlert(
            alertType = "HOLDING",
            icao24 = latest.icao24,
            callsign = latest.callsign,
            airport = airport.icao,
            detectedAt = latest.time,
            windowStart = recent.head.time,
            totalTurnDeg = math.round(turn * 10) / 10.0,
            altitudeM = latest.altitudeM,
            lat = latest.lat,
            lon = latest.lon,
            positions = recent.size
          )
        }

      val lastAlertTime = alert.map(_.detectedAt).getOrElse(state.lastAlertTime)
      (HoldingState(recent, lastAlertTime), alert)
    }
  }
}