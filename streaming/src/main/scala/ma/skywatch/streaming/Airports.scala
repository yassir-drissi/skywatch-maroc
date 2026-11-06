package ma.skywatch.streaming

/** Un aéroport surveillé (mêmes valeurs que ingestion/skywatch_ingestion/config.py). */
final case class Airport(icao: String, name: String, city: String, lat: Double, lon: Double)

object Airports {

  /** Rayon autour d'un aéroport pour compter son trafic (zone d'approche), en km. */
  val TerminalAreaKm = 60.0

  val All: Seq[Airport] = Seq(
    Airport("GMMN", "Mohammed V", "Casablanca", 33.3675, -7.5900),
    Airport("GMMX", "Menara", "Marrakech", 31.6069, -8.0363),
    Airport("GMME", "Rabat-Salé", "Rabat", 34.0515, -6.7515),
    Airport("GMFF", "Fès-Saïss", "Fès", 33.9273, -4.9779),
    Airport("GMTT", "Ibn Battouta", "Tanger", 35.7269, -5.9169),
    Airport("GMAD", "Al Massira", "Agadir", 30.3250, -9.4131)
  )

  /** L'aéroport le plus proche d'un point, avec la distance en km. */
  def nearest(lat: Double, lon: Double): (Airport, Double) =
    All.map(a => (a, Geo.haversineKm(lat, lon, a.lat, a.lon))).minBy(_._2)

  /** L'aéroport le plus proche, seulement s'il est à moins de `radiusKm`. */
  def within(lat: Double, lon: Double, radiusKm: Double): Option[Airport] = {
    val (airport, km) = nearest(lat, lon)
    if (km <= radiusKm) Some(airport) else None
  }
}