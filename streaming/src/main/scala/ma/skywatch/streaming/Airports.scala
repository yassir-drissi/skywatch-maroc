package ma.skywatch.streaming

/** Un aéroport surveillé (mêmes valeurs que ingestion/skywatch_ingestion/config.py).
  *
  * elevationM : altitude de la piste au-dessus de la mer (valeur arrondie). Nécessaire parce
  * qu'OpenSky donne l'altitude au-dessus de la mer : à Fès (579 m), un avion à 900 m n'est
  * qu'à 320 m au-dessus de la piste.
  */
final case class Airport(icao: String, name: String, city: String, lat: Double, lon: Double, elevationM: Double)

object Airports {

  /** Rayon autour d'un aéroport pour compter son trafic (zone d'approche), en km. */
  val TerminalAreaKm = 60.0

  val All: Seq[Airport] = Seq(
    Airport("GMMN", "Mohammed V", "Casablanca", 33.3675, -7.5900, 200),
    Airport("GMMX", "Menara", "Marrakech", 31.6069, -8.0363, 466),
    Airport("GMME", "Rabat-Salé", "Rabat", 34.0515, -6.7515, 84),
    Airport("GMFF", "Fès-Saïss", "Fès", 33.9273, -4.9779, 579),
    Airport("GMTT", "Ibn Battouta", "Tanger", 35.7269, -5.9169, 21),
    Airport("GMAD", "Al Massira", "Agadir", 30.3250, -9.4131, 23)
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