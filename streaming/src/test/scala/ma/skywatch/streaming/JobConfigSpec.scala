package ma.skywatch.streaming

import org.scalatest.funsuite.AnyFunSuite

class JobConfigSpec extends AnyFunSuite {

  test("valeurs par défaut pour lancer le job depuis le PC") {
    val c = JobConfig.fromEnv(Map.empty)
    assert(c.kafkaBootstrap == "localhost:9092")
    assert(c.flightsTopic == "flights.raw")
    assert(c.weatherTopic == "weather.raw")
    assert(c.alertsTopic == "flights.alerts")
    assert(c.startingOffsets == "earliest")
  }

  test("dans Docker, l'adresse Kafka et le lake viennent des variables d'environnement") {
    val c = JobConfig.fromEnv(Map(
      "KAFKA_BOOTSTRAP_SERVERS" -> "kafka:29092",
      "LAKE_PATH"               -> "/opt/skywatch/data/lake/"
    ))
    assert(c.kafkaBootstrap == "kafka:29092")
    assert(c.lakePath == "/opt/skywatch/data/lake") // le « / » final est retiré
  }

  test("chemins Bronze et checkpoints") {
    val c = JobConfig.fromEnv(Map("LAKE_PATH" -> "/lake"))
    assert(c.bronzePath("flights") == "/lake/bronze/flights")
    assert(c.checkpointPath("bronze_flights") == "/lake/_checkpoints/bronze_flights")
  }
}