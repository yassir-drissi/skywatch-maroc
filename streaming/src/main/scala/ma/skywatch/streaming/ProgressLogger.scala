package ma.skywatch.streaming

import org.apache.spark.sql.streaming.StreamingQueryListener

/** Affiche une ligne par micro-batch dans les logs du conteneur, pour chaque query. */
class ProgressLogger extends StreamingQueryListener {

  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit =
    println(s"[stream] query ${event.name} démarrée")

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    val p = event.progress
    println(s"[stream] ${p.name} batch=${p.batchId} lignes=${p.numInputRows}")
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit =
    println(s"[stream] query ${event.id} arrêtée ${event.exception.getOrElse("")}")
}