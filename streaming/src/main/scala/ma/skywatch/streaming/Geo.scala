package ma.skywatch.streaming

/** Calculs géographiques utilisés par la détection d'anomalies. */
object Geo {

  private val EarthRadiusKm = 6371.0

  /** Distance réelle sur la Terre entre deux points, en kilomètres (formule haversine). */
  def haversineKm(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
    val dLat = math.toRadians(lat2 - lat1)
    val dLon = math.toRadians(lon2 - lon1)
    val a = math.pow(math.sin(dLat / 2), 2) +
      math.cos(math.toRadians(lat1)) * math.cos(math.toRadians(lat2)) * math.pow(math.sin(dLon / 2), 2)
    2 * EarthRadiusKm * math.asin(math.sqrt(a))
  }

  /** Virage entre deux caps, en degrés, toujours entre -180 et +180.
    *
    * Positif = virage à droite, négatif = virage à gauche.
    * Exemple : de 350° à 10°, l'avion a tourné de +20° (et pas de -340°).
    */
  def headingDelta(from: Double, to: Double): Double =
    ((to - from) % 360 + 540) % 360 - 180
}