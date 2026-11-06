package ma.skywatch.streaming

/** Ce que Spark garde en mémoire pour chaque avion (détection des remises de gaz). */
final case class GoAroundState(positions: Seq[Position], lastAlertTime: Long)

object GoAroundState {
  val Empty: GoAroundState = GoAroundState(Seq.empty, 0L)
}

/** Une alerte de remise de gaz envoyée dans le topic flights.alerts. */
final case class GoAroundAlert(
    alertType: String,
    icao24: String,
    callsign: Option[String],
    airport: String,
    detectedAt: Long,        // secondes Unix de la dernière position
    lowestAt: Long,          // secondes Unix du point le plus bas
    lowestAltitudeM: Double, // altitude au-dessus de la mer au point le plus bas
    lowestAglM: Double,      // hauteur au-dessus de la piste au point le plus bas
    climbM: Double,          // de combien l'avion est remonté après le point le plus bas
    lat: Double,             // position du point le plus bas
    lon: Double,
    positions: Int
)

/** Détection des remises de gaz, en Scala pur (sans Spark) pour pouvoir la tester.
  *
  * Une remise de gaz : l'avion descend vers une piste, passe bas (moins de 600 m au-dessus
  * de la piste, à moins de 15 km de l'aéroport), puis remonte nettement SANS avoir touché
  * le sol. S'il a touché le sol, c'est un atterrissage normal.
  */
object GoAroundDetector {

  val WindowS: Long           = 300   // on regarde les 5 dernières minutes
  val ApproachRadiusKm        = 15.0  // le point bas doit être près de la piste
  val MaxLowPointAglM         = 600.0 // hauteur maximale du point bas au-dessus de la piste
  val MinDescentM             = 150.0 // il est d'abord descendu d'au moins 150 m
  val MinClimbM               = 300.0 // puis il est remonté d'au moins 300 m
  val CooldownS: Long         = 900   // au plus une alerte toutes les 15 minutes par avion

  /** Cherche une remise de gaz dans des positions triées par temps. */
  def detect(recent: Seq[Position]): Option[GoAroundAlert] =
    if (recent.exists(_.onGround)) None // il a touché la piste : atterrissage (ou décollage)
    else {
      val flying = recent.filter(_.altitudeM.isDefined)
      if (flying.size < 3) None
      else {
        val lowest   = flying.minBy(_.altitudeM.get)
        val lowAlt   = lowest.altitudeM.get
        val before   = flying.filter(_.time < lowest.time).map(_.altitudeM.get)
        val after    = flying.filter(_.time > lowest.time).map(_.altitudeM.get)
        val descent  = if (before.isEmpty) 0.0 else before.max - lowAlt
        val climb    = if (after.isEmpty) 0.0 else after.max - lowAlt
        val latest   = recent.last

        Airports
          .within(lowest.lat, lowest.lon, ApproachRadiusKm)
          .filter(a => lowAlt - a.elevationM <= MaxLowPointAglM && descent >= MinDescentM && climb >= MinClimbM)
          .map { a =>
            GoAroundAlert(
              alertType = "GO_AROUND",
              icao24 = latest.icao24,
              callsign = latest.callsign,
              airport = a.icao,
              detectedAt = latest.time,
              lowestAt = lowest.time,
              lowestAltitudeM = lowAlt,
              lowestAglM = math.round(lowAlt - a.elevationM).toDouble,
              climbM = math.round(climb).toDouble,
              lat = lowest.lat,
              lon = lowest.lon,
              positions = recent.size
            )
          }
      }
    }

  /** Ajoute les nouvelles positions à l'état de l'avion et décide s'il faut une alerte. */
  def update(state: GoAroundState, batch: Seq[Position]): (GoAroundState, Option[GoAroundAlert]) = {
    // anciennes + nouvelles positions, une seule par instantané, dans l'ordre du temps
    val merged = (state.positions ++ batch).groupBy(_.time).values.map(_.head).toSeq.sortBy(_.time)

    if (merged.isEmpty) (state, None)
    else {
      val latest     = merged.last
      val recent     = merged.filter(_.time > latest.time - WindowS) // les 5 dernières minutes
      val cooledDown = latest.time - state.lastAlertTime >= CooldownS
      val alert      = if (cooledDown) detect(recent) else None

      val lastAlertTime = alert.map(_.detectedAt).getOrElse(state.lastAlertTime)
      (GoAroundState(recent, lastAlertTime), alert)
    }
  }
}