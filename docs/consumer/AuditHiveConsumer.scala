package com.hcsc.bluepcs.consumer

// =============================================================================
// AuditHiveConsumer — standalone Spark DStream job: bridge audit topic -> Hive
// =============================================================================
// Consumes the bridge's Kafka audit topic (see docs/AUDIT.md for the event
// schema and guarantees) and appends the events to a partitioned Hive table,
// replacing the old Talend-era audit table feed (cv_Hive_Audit_tblname /
// bluepcs_mq_listener).
//
// Design, mirroring the conventions of BluepcsPMMPLusConsumer:
//   - Direct Kafka stream, enable.auto.commit=false; offsets are committed back
//     to Kafka (commitAsync) only AFTER the batch is written to Hive ->
//     at-least-once. Duplicates are expected and harmless: auditEventId is
//     unique per event, so downstream queries dedupe with
//     ROW_NUMBER() OVER (PARTITION BY audit_event_id) = 1 (or SELECT DISTINCT).
//   - The job must NEVER crash-loop on bad input (audit is best-effort end to
//     end): unparseable values are still landed, with the raw value preserved
//     in raw_value and null typed columns — nothing on the topic is dropped.
//   - One Hive write per micro-batch => one file-set per batch. Size the batch
//     interval (default 300s) for audit volume; audit events are small and
//     latency-tolerant, so prefer LONGER intervals to avoid small files.
//   - metadata is landed as a JSON string (get_json_object() to query) so the
//     table never needs a schema change when the bridge adds a metadata key.
//
// -----------------------------------------------------------------------------
// Hive DDL (create once, before first run; adjust db/table/location to env):
//
//   CREATE TABLE IF NOT EXISTS bluepcs.bridge_audit_event (
//     audit_event_id        STRING,   -- unique per event: dedupe key
//     event_id              STRING,   -- joins notification + HDFS payload file
//     bridge_event_id       STRING,   -- groups one processing attempt
//     original_mq_message_id STRING,
//     transaction_id        STRING,
//     event_type            STRING,   -- MESSAGE_RECEIVED, ..., null if unparseable
//     description           STRING,
//     error_message         STRING,
//     metadata_json         STRING,   -- JSON map; get_json_object(metadata_json,'$.deliveryCount')
//     event_timestamp       STRING,   -- ISO-8601 UTC as emitted by the bridge
//     kafka_partition       INT,      -- lineage / debugging
//     kafka_offset          BIGINT,
//     raw_value             STRING    -- set ONLY when the value failed to parse
//   )
//   PARTITIONED BY (event_dt STRING)  -- yyyy-MM-dd from event_timestamp (UTC);
//                                     -- ingestion date when unparseable
//   STORED AS ORC;
//
// -----------------------------------------------------------------------------
// spark-submit (copy the Kafka JAAS/keytab confs from the existing consumer's
// launcher — this job needs exactly the same Kafka + Hive Kerberos setup):
//
//   spark-submit \
//     --class com.hcsc.bluepcs.consumer.AuditHiveConsumer \
//     --master yarn --deploy-mode cluster \
//     --principal <principal> --keytab <keytab> \
//     --files kafka_client_jaas.conf,<truststore.jks> \
//     --conf spark.driver.extraJavaOptions=-Djava.security.auth.login.config=kafka_client_jaas.conf \
//     --conf spark.executor.extraJavaOptions=-Djava.security.auth.login.config=kafka_client_jaas.conf \
//     --conf spark.bluepcs.audit.bootstrap.servers=<host:9093,...> \
//     --conf spark.bluepcs.audit.topic=<audit topic, e.g. MOCK01_BRIDGE_AUDIT_TEST> \
//     --conf spark.bluepcs.audit.group.id=bluepcs-audit-hive-consumer \
//     --conf spark.bluepcs.audit.hive.table=bluepcs.bridge_audit_event \
//     --conf spark.bluepcs.audit.truststore=<truststore.jks name from --files> \
//     --conf spark.bluepcs.audit.truststore.password=<pw> \
//     --conf spark.bluepcs.audit.batch.seconds=300 \
//     <consumer-jar>
//
// First run starts from the EARLIEST retained offset (auto.offset.reset), so
// nothing already on the topic is missed; afterwards the committed group
// offset wins. Schedule/supervise like the other cluster jobs (yarn cluster
// mode restarts the driver on failure; or wrap in the existing job runner).
// =============================================================================

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.slf4j.LoggerFactory

import java.time.format.DateTimeFormatter
import java.time.{Instant, ZoneOffset}

object AuditHiveConsumer {

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  /** One row of the Hive table; field order MUST match the DDL column order. */
  final case class AuditRow(
      audit_event_id: String,
      event_id: String,
      bridge_event_id: String,
      original_mq_message_id: String,
      transaction_id: String,
      event_type: String,
      description: String,
      error_message: String,
      metadata_json: String,
      event_timestamp: String,
      kafka_partition: Int,
      kafka_offset: Long,
      raw_value: String,
      event_dt: String // partition column last, as insertInto requires
  )

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    def cfg(key: String, default: String = null): String = {
      val v = conf.get(s"spark.bluepcs.audit.$key", default)
      require(v != null, s"Missing required conf spark.bluepcs.audit.$key")
      v
    }

    val bootstrapServers = cfg("bootstrap.servers")
    val topic            = cfg("topic")
    val groupId          = cfg("group.id", "bluepcs-audit-hive-consumer")
    val hiveTable        = cfg("hive.table", "bluepcs.bridge_audit_event")
    val batchSeconds     = cfg("batch.seconds", "300").toLong

    val spark = SparkSession.builder()
      .config(conf)
      .appName("BluepcsAuditHiveConsumer")
      .enableHiveSupport()
      .getOrCreate()

    val ssc = new StreamingContext(spark.sparkContext, Seconds(batchSeconds))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers"            -> bootstrapServers,
      "key.deserializer"             -> classOf[StringDeserializer],
      "value.deserializer"           -> classOf[StringDeserializer],
      "group.id"                     -> groupId,
      "auto.offset.reset"            -> "earliest",
      "enable.auto.commit"           -> (false: java.lang.Boolean),
      "security.protocol"            -> cfg("security.protocol", "SASL_SSL"),
      "sasl.mechanism"               -> cfg("sasl.mechanism", "GSSAPI"),
      "sasl.kerberos.service.name"   -> cfg("kerberos.service.name", "kafka"),
      "ssl.truststore.location"      -> cfg("truststore"),
      "ssl.truststore.password"      -> cfg("truststore.password")
    )

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Seq(topic), kafkaParams)
    )

    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

        val rows = rdd.mapPartitions { it =>
          val mapper = new ObjectMapper() // one per partition, not per record
          it.map(record => toRow(record, mapper))
        }

        import spark.implicits._
        val df = rows.toDF()

        // Dynamic partitioning: each batch may span midnight (or carry late events)
        spark.sql("SET hive.exec.dynamic.partition.mode=nonstrict")
        df.write.mode("append").insertInto(hiveTable)

        val total = offsetRanges.map(r => r.untilOffset - r.fromOffset).sum
        logger.info("@@@ AUDIT->HIVE batch written: {} events to {} ({})",
          Long.box(total), hiveTable, offsetRanges.mkString(", "))

        // Commit only after the Hive write succeeded -> at-least-once.
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }

  private val DateFmt = DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC)

  /**
   * Best-effort mapping of one Kafka record to a Hive row. NEVER throws:
   * an unparseable value lands with raw_value set and typed columns null,
   * partitioned by ingestion date. The audit trail must not crash-loop the
   * job — landing something is always better than landing nothing.
   */
  private[consumer] def toRow(record: ConsumerRecord[String, String], mapper: ObjectMapper): AuditRow =
    try {
      val n = mapper.readTree(record.value())
      require(n != null && n.isObject, "not a JSON object")
      val ts = n.path("timestamp").asText("")
      AuditRow(
        audit_event_id         = textOrNull(n, "auditEventId"),
        event_id               = textOrNull(n, "eventId"),
        bridge_event_id        = textOrNull(n, "bridgeEventId"),
        original_mq_message_id = textOrNull(n, "originalMqMessageId"),
        transaction_id         = textOrNull(n, "transactionId"),
        event_type             = textOrNull(n, "eventType"),
        description            = textOrNull(n, "description"),
        error_message          = textOrNull(n, "errorMessage"),
        metadata_json          = if (n.hasNonNull("metadata")) n.get("metadata").toString else null,
        event_timestamp        = if (ts.isEmpty) null else ts,
        kafka_partition        = record.partition(),
        kafka_offset           = record.offset(),
        raw_value              = null,
        event_dt               = eventDate(ts)
      )
    } catch {
      case e: Exception =>
        logger.warn("@@@ Unparseable audit value at {}-{}: {} — landing raw",
          Int.box(record.partition()), Long.box(record.offset()), e.getMessage)
        AuditRow(null, null, null, null, null, null, null, null, null, null,
          record.partition(), record.offset(), record.value(),
          DateFmt.format(Instant.now()))
    }

  /** Partition date from the event's own UTC timestamp; ingestion date as fallback. */
  private def eventDate(isoTimestamp: String): String =
    try DateFmt.format(Instant.parse(isoTimestamp))
    catch { case _: Exception => DateFmt.format(Instant.now()) }

  private def textOrNull(n: JsonNode, field: String): String =
    if (n.hasNonNull(field)) n.get(field).asText() else null
}

// =============================================================================
// Build & operate notes
// =============================================================================
// Dependencies (match the cluster's Spark/Scala versions — copy from the
// existing consumer's build):
//   - spark-core, spark-sql, spark-hive, spark-streaming        (provided)
//   - spark-streaming-kafka-0-10_<scala.version>                (bundled)
//   - jackson-databind is already on Spark's classpath
//
// Useful queries once data lands:
//   -- latest state per message (deduped):
//   SELECT event_id, max(event_timestamp) AS last_seen,
//          collect_set(event_type) AS events
//   FROM bluepcs.bridge_audit_event
//   WHERE event_dt >= date_sub(current_date, 7)
//   GROUP BY event_id;
//
//   -- messages stuck in a redelivery loop (many attempts, no completion):
//   SELECT event_id, count(DISTINCT bridge_event_id) AS attempts
//   FROM bluepcs.bridge_audit_event
//   WHERE event_dt >= date_sub(current_date, 1)
//   GROUP BY event_id
//   HAVING count(DISTINCT bridge_event_id) > 3
//      AND max(CASE WHEN event_type = 'PROCESSING_COMPLETED' THEN 1 ELSE 0 END) = 0;
//
//   -- rows that failed to parse (should be rare/none):
//   SELECT * FROM bluepcs.bridge_audit_event
//   WHERE raw_value IS NOT NULL AND event_dt = current_date;
//
// Operational notes:
//   - At-least-once => duplicates possible after a restart between Hive write
//     and offset commit. Dedupe on audit_event_id in queries (see header).
//   - The topic side needs a CONSUME/read ACL for this job's principal and
//     group.id — request alongside the bridge's produce ACL.
//   - Batch interval is the small-files knob: 300s at audit volume is ~one
//     modest ORC file-set per 5 minutes per partition. Compact old partitions
//     with a periodic INSERT OVERWRITE ... SELECT DISTINCT if needed.
//   - Monitoring: alert on YARN app absence/restarts like the other streaming
//     jobs; "@@@ AUDIT->HIVE batch written" is the per-batch liveness marker.
// =============================================================================
