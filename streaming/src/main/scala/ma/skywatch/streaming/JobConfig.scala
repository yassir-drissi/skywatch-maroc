package ma.skywatch.streaming

/** Configuration des jobs Spark, lue depuis les variables d'environnement.
  *
  * Même principe que config.py côté Python : aucune adresse ni chemin en dur
  * dans le code des jobs.
  */
final case class JobConfig(
    kafkaBootstrap: String,
    lakePath: String,
    flightsTopic: String,
    weatherTopic: String,
    alertsTopic: String,
    startingOffsets: String,
    triggerInterval: String
) {

  /** Dossier d'une table Bronze, ex. data/lake/bronze/flights */
  def bronzePath(table: String): String = s"$lakePath/bronze/$table"

  /** Dossier de checkpoint d'une query : Spark y note où il en est dans Kafka. */
  def checkpointPath(query: String): String = s"$lakePath/_checkpoints/$query"
}

object JobConfig {

  def fromEnv(env: Map[String, String] = sys.env): JobConfig =
    JobConfig(
      kafkaBootstrap  = env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
      lakePath        = env.getOrElse("LAKE_PATH", "data/lake").stripSuffix("/"),
      flightsTopic    = env.getOrElse("TOPIC_FLIGHTS_RAW", "flights.raw"),
      weatherTopic    = env.getOrElse("TOPIC_WEATHER_RAW", "weather.raw"),
      alertsTopic     = env.getOrElse("TOPIC_FLIGHTS_ALERTS", "flights.alerts"),
      startingOffsets = env.getOrElse("STARTING_OFFSETS", "earliest"),
      triggerInterval = env.getOrElse("BRONZE_TRIGGER", "1 minute")
    )
}