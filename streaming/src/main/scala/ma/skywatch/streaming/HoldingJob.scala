package ma.skywatch.streaming

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/** Branche HoldingDetector sur le flux Spark : un état par avion, alertes vers Kafka. */
object HoldingJob {

  /** Colonnes de flights.raw → Dataset typé de Position. */
  def toPositions(spark: SparkSession, flights: DataFrame): Dataset[Position] = {
    import spark.implicits._
    flights
      .where(col("latitude").isNotNull && col("longitude").isNotNull)
      .select(
        col("icao24"),
        col("callsign"),
        col("snapshot_time").as("time"),
        col("event_time").as("eventTime"),
        col("latitude").as("lat"),
        col("longitude").as("lon"),
        col("baro_altitude").as("altitudeM"),
        col("true_track").as("track"),
        coalesce(col("on_ground"), lit(false)).as("onGround")
      )
      .as[Position]
  }

  /** Appelée par Spark pour chaque avion qui a de nouvelles positions (ou dont l'état expire). */
  def updateGroup(icao24: String, rows: Iterator[Position], state: GroupState[HoldingState]): Iterator[HoldingAlert] =
    if (state.hasTimedOut) {
      state.remove() // plus aucune position depuis 10 minutes : on oublie cet avion
      Iterator.empty
    } else {
      val previous      = state.getOption.getOrElse(HoldingState.Empty)
      val (next, alert) = HoldingDetector.update(previous, rows.toList)
      state.update(next)
      next.positions.lastOption.foreach { p =>
        val expireMs = (p.time + HoldingDetector.WindowS) * 1000L
        state.setTimeoutTimestamp(math.max(expireMs, state.getCurrentWatermarkMs() + 1000L))
      }
      alert.iterator
    }

  /** Alerte → message Kafka (clé icao24, valeur JSON avec des noms en snake_case). */
  def toKafkaMessages(alerts: Dataset[HoldingAlert]): DataFrame =
    alerts.select(
      col("icao24").as("key"),
      to_json(
        struct(
          lit(1).as("schema_version"),
          col("alertType").as("alert_type"),
          col("icao24"),
          col("callsign"),
          col("airport"),
          col("detectedAt").as("detected_at"),
          col("windowStart").as("window_start"),
          col("totalTurnDeg").as("total_turn_deg"),
          col("altitudeM").as("altitude_m"),
          col("lat").as("latitude"),
          col("lon").as("longitude"),
          col("positions")
        )
      ).as("value")
    )

  def startHolding(spark: SparkSession, config: JobConfig): StreamingQuery = {
    import spark.implicits._

    val kafka   = BronzeJob.readTopic(spark, config, config.flightsTopic)
    val flights = FlightsStream.parse(kafka).withWatermark("event_time", TrafficJob.Watermark)

    val alerts = toPositions(spark, flights)
      .groupByKey(_.icao24) // un groupe (et un état) par avion
      .flatMapGroupsWithState[HoldingState, HoldingAlert](OutputMode.Append(), GroupStateTimeout.EventTimeTimeout())(
        updateGroup _
      )

    toKafkaMessages(alerts).writeStream
      .queryName("holding_alerts")
      .format("kafka")
      .option("kafka.bootstrap.servers", config.kafkaBootstrap)
      .option("topic", config.alertsTopic)
      .option("checkpointLocation", config.checkpointPath("holding_alerts"))
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(config.triggerInterval))
      .start()
  }
}