package com.hcsc.bluepcs.consumer

// =============================================================================
// ConsumerAuditEmitter — drop-in for BluepcsPMMPLusConsumer (Spark DStream job)
// =============================================================================
// Emits the CONSUMER-STAGE audit events that close the end-to-end trail
// (see docs/AUDIT.md, "Consumer stages"): the bridge records everything up to
// PROCESSING_COMPLETED; this emitter adds, keyed by the SAME eventId on the
// SAME audit topic:
//
//   CLAIM_CHECK_RESOLVED   - HDFS payload fetched + checksum verified
//   CLAIM_CHECK_SKIPPED    - file missing -> already-processed duplicate
//   HIVE_LOAD_COMPLETED    - batch containing this eventId loaded to the Hive
//                            product tables (emitted BEFORE the offset commit)
//   HIVE_LOAD_FAILED       - batch load failed; the batch will retry
//
// Design rules (mirror the bridge's SafeAuditPublisher semantics):
//   - BEST-EFFORT: every emit failure is logged (@@@ AUDIT EMIT FAILED) and
//     swallowed. Audit must NEVER fail or slow a batch.
//   - DRIVER-SIDE ONLY: call from foreachRDD driver code, never inside
//     mapPartitions. One producer for the job lifetime, lazily created.
//     Per-batch volume is small (~= batch message count), so driver-side
//     emission costs milliseconds.
//   - Bridge messages only: legacy Talend inline messages have no eventId and
//     get no consumer events (by decision — they phase out with Talend).
//
// JSON layout matches com.hcsc.bridge.audit.AuditEvent exactly, so these
// events land in the same Hive audit table (bridge_audit_event) through the
// same AuditHiveConsumer with no schema change. bridgeEventId/transactionId
// are null for consumer events; metadata carries kafkaPartition/kafkaOffset/
// batchTime.
//
// ACL prerequisite: this job's principal needs PRODUCE on the audit topic
// (request alongside the consume ACL for the audit Hive consumer).
// =============================================================================

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.slf4j.LoggerFactory

import java.time.Instant
import java.util.{Properties, UUID}

object ConsumerAuditEmitter {

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  @volatile private var producer: KafkaProducer[String, String] = _
  @volatile private var auditTopic: String = _

  // Outage cooldown: a synchronous send failure (e.g. metadata timeout after
  // max.block.ms with brokers down) suppresses further emits for this long.
  // Without it EVERY send blocks the full max.block.ms during an audit-Kafka
  // outage — a 500-message batch would stall the driver ~42 minutes.
  private val SuppressMillis = 60000L
  @volatile private var suppressUntil: Long = 0L

  /**
   * Initialize once at job startup (driver), BEFORE the streaming context
   * starts. kafkaParams: reuse the job's existing consumer security settings
   * (bootstrap.servers, security.protocol, sasl.*, ssl.*) — producer-specific
   * keys are set here.
   */
  def init(kafkaParams: Map[String, Object], topic: String): Unit = synchronized {
    if (producer == null) {
      val props = new Properties()
      val carryOver = Seq(
        "bootstrap.servers", "security.protocol", "sasl.mechanism",
        "sasl.kerberos.service.name", "ssl.truststore.location",
        "ssl.truststore.password", "ssl.truststore.type")
      carryOver.foreach { k =>
        kafkaParams.get(k).foreach(v => props.put(k, v.toString))
      }
      props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      props.put("acks", "all")
      // Fail fast rather than stall the driver when Kafka is unreachable:
      // audit is best-effort, the batch must proceed at full speed regardless.
      props.put("max.block.ms", "5000")
      props.put("delivery.timeout.ms", "30000")
      producer = new KafkaProducer[String, String](props)
      auditTopic = topic
      logger.info("@@@ ConsumerAuditEmitter initialized for topic {}", topic)
    }
  }

  /** Call from a shutdown hook / StreamingContext stop; safe to skip. */
  def close(): Unit = synchronized {
    if (producer != null) {
      // Bounded: an unbounded close() in a shutdown hook hangs JVM exit when Kafka is down
      try producer.close(java.time.Duration.ofSeconds(5)) catch { case _: Exception => () }
      producer = null
    }
  }

  def claimCheckResolved(eventId: String, partition: Int, offset: Long, batchTime: Long): Unit =
    emit("CLAIM_CHECK_RESOLVED", eventId, "Claim-check payload fetched and checksum verified",
      errorMessage = null, partition, offset, batchTime)

  def claimCheckSkipped(eventId: String, partition: Int, offset: Long, batchTime: Long): Unit =
    emit("CLAIM_CHECK_SKIPPED", eventId, "HDFS file missing - already-processed duplicate, skipped",
      errorMessage = null, partition, offset, batchTime)

  def hiveLoadCompleted(eventId: String, partition: Int, offset: Long, batchTime: Long): Unit =
    emit("HIVE_LOAD_COMPLETED", eventId, "Batch loaded into Hive product tables",
      errorMessage = null, partition, offset, batchTime)

  def hiveLoadFailed(eventId: String, partition: Int, offset: Long, batchTime: Long, error: String): Unit =
    emit("HIVE_LOAD_FAILED", eventId, "Batch load into Hive product tables failed",
      errorMessage = error, partition, offset, batchTime)

  /**
   * Best-effort fire-and-forget. Never throws. The send callback only logs;
   * no state depends on audit delivery. A SYNCHRONOUS send failure (broker
   * unreachable -> metadata timeout after max.block.ms) trips the cooldown so
   * the remaining events of the batch are skipped instead of each blocking
   * the driver for the full max.block.ms. Async callback failures do not trip
   * it — they never blocked the driver.
   */
  private def emit(eventType: String, eventId: String, description: String,
                   errorMessage: String, partition: Int, offset: Long, batchTime: Long): Unit =
    try {
      if (System.currentTimeMillis() < suppressUntil) return
      require(producer != null, "ConsumerAuditEmitter.init(...) not called")
      val json = buildJson(eventType, eventId, description, errorMessage, partition, offset, batchTime)
      // Explicit Callback (not a lambda): Scala 2.11 has no SAM conversion
      producer.send(new ProducerRecord[String, String](auditTopic, eventId, json),
        new Callback {
          override def onCompletion(md: RecordMetadata, ex: Exception): Unit =
            if (ex != null)
              logger.warn("@@@ AUDIT EMIT FAILED (async): {} {} - {}", eventType, eventId, ex.getMessage)
        })
    } catch {
      case e: Exception =>
        suppressUntil = System.currentTimeMillis() + SuppressMillis
        logger.warn("@@@ AUDIT EMIT FAILED, suppressing emits for {}s: {} {} - {}",
          Long.box(SuppressMillis / 1000), eventType, eventId, e.getMessage)
    }

  /**
   * Hand-built JSON (no Jackson dependency on the emit path; field set matches
   * com.hcsc.bridge.audit.AuditEvent). eventType/description are controlled
   * internal values, but eventId and errorMessage are MESSAGE-DERIVED on some
   * paths (CLAIM_CHECK_SKIPPED re-parses eventId from the raw notification;
   * anyone who can write to the product topic controls it) — both are escaped
   * so a crafted value cannot inject fields (e.g. a forged HIVE_LOAD_COMPLETED
   * that would blind the gap check) or corrupt the JSON.
   */
  private def buildJson(eventType: String, eventId: String, description: String,
                        errorMessage: String, partition: Int, offset: Long, batchTime: Long): String = {
    val err = if (errorMessage == null) "null" else "\"" + escape(errorMessage) + "\""
    s"""{"auditEventId":"${UUID.randomUUID()}",""" +
      s""""eventId":"${escape(eventId)}",""" +
      s""""bridgeEventId":null,""" +
      s""""originalMqMessageId":null,""" +
      s""""messageId":null,""" +
      s""""transactionId":null,""" +
      s""""eventType":"$eventType",""" +
      s""""description":"${escape(description)}",""" +
      s""""metadata":{"kafkaPartition":$partition,"kafkaOffset":$offset,"batchTime":$batchTime},""" +
      s""""timestamp":"${Instant.now()}",""" +
      s""""errorMessage":$err}"""
  }

  private def escape(s: String): String =
    s.flatMap {
      case '"'  => "\\\""
      case '\\' => "\\\\"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case c if c < ' ' => "\\u%04x".format(c.toInt)
      case c    => c.toString
    }
}

// =============================================================================
// Wiring into BluepcsPMMPLusConsumer
// =============================================================================
// This EXTENDS the BridgeMessageResolver wiring (see BridgeMessageResolver.scala);
// apply that first, then layer this on.
//
// 1) At job startup, after the kafka consumer params map is built and before
//    ssc.start():
//
//      ConsumerAuditEmitter.init(kafkaParams, auditTopicName)
//      // auditTopicName from job config, e.g. DAS_PRODUCT_PHM_PRODUCT_AUDIT_TEST
//
// 2) Inside stream.foreachRDD, replace the resolver block from
//    BridgeMessageResolver.scala's wiring notes with the version below. The
//    resolution result is widened to carry the skip/resolve disposition so it
//    can be audited AND so skipped duplicates still get their event:
//
//      val batchTime = System.currentTimeMillis()
//
//      val resolvedRDD =
//        inputRDD.mapPartitions { it =>
//          val fs = FileSystem.get(new Configuration())
//          val mapper = new ObjectMapper()
//          it.map { case (partition, offset, raw) =>
//            BridgeMessageResolver.resolveOrSkip(raw, fs, mapper) match {
//              case Some(r) => (partition, offset, r.json, r.eventId, "resolved", r.isClaimCheck)
//              case None    =>
//                // eventId for the skip event: re-parse the claim-check envelope
//                // (reuse the partition's mapper; guard the parse — the raw value is
//                // untrusted and a throw here would fail the whole batch)
//                val id =
//                  try mapper.readTree(raw).path("eventId").asText(null)
//                  catch { case _: Exception => null }
//                (partition, offset, null, Option(id), "skipped", true)
//            }
//          }
//        }.persist()
//
//      // --- audit: claim-check dispositions (driver-side, small collect) ---
//      val dispositions = resolvedRDD
//        .filter(_._6)                                   // claim-check only
//        .map(t => (t._4, t._1, t._2, t._5))             // (eventId, part, off, disp)
//        .collect()
//      dispositions.foreach {
//        case (Some(id), p, o, "resolved") => ConsumerAuditEmitter.claimCheckResolved(id, p, o, batchTime)
//        case (Some(id), p, o, "skipped")  => ConsumerAuditEmitter.claimCheckSkipped(id, p, o, batchTime)
//        case _                            => () // claim-check without readable eventId: nothing to key on
//      }
//
//      val processRDD = resolvedRDD.filter(_._5 == "resolved").map(t => (t._1, t._2, t._3))
//
//      try {
//        BluepcsPMMPLusProcessor.processJSONMessages(
//          common_conf, bluepcs_conf, processRDD, kafkaOffsetDetailsDF, env)
//
//        // --- audit: Hive load success, BEFORE the HBase offset commit ---
//        dispositions.foreach {
//          case (Some(id), p, o, "resolved") => ConsumerAuditEmitter.hiveLoadCompleted(id, p, o, batchTime)
//          case _                            => ()
//        }
//      } catch {
//        case e: Exception =>
//          dispositions.foreach {
//            case (Some(id), p, o, "resolved") =>
//              ConsumerAuditEmitter.hiveLoadFailed(id, p, o, batchTime, e.getMessage)
//            case _ => ()
//          }
//          throw e   // batch retry semantics UNCHANGED - offsets not committed
//      } finally {
//        resolvedRDD.unpersist()
//      }
//
// Behavior notes:
//   - HIVE_LOAD_COMPLETED is per-batch granularity: it asserts "the batch this
//     eventId belonged to was fully processed". That is the honest claim a
//     batch-oriented processor can make, and exactly what gap detection needs.
//   - On batch retry after HIVE_LOAD_FAILED, the whole event sequence repeats
//     (new auditEventIds, same eventId) — normal at-least-once duplicates,
//     dedupe downstream on audit_event_id / latest event_timestamp.
//   - Legacy inline messages flow through processRDD untouched and emit no
//     audit events (they carry no eventId).
//   - If the audit topic is unreachable: the FIRST send of a batch blocks up to
//     max.block.ms (5s), fails synchronously, and trips a 60s emit-suppression
//     cooldown — the rest of the batch's events are skipped, not blocked. (The
//     producer has NO latched error state of its own: without the cooldown,
//     every send would block the full max.block.ms.) Batches process at full
//     speed; suppressed audit events are lost, which is acceptable best-effort
//     behavior and logged via @@@ AUDIT EMIT FAILED.
// =============================================================================
