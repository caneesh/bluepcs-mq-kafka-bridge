package com.hcsc.bluepcs.consumer

// =============================================================================
// BridgeMessageResolver — drop-in for BluepcsPMMPLusConsumer (Spark DStream job)
// =============================================================================
// The Kafka topic carries TWO message formats during the Talend → bridge
// migration:
//
//   1. Legacy Talend (inline):    {"changeEventTimeStamp": "...",
//                                  "RestAPIResponse": { ...full document... },
//                                  "changeEventTypeName": "..."}
//
//   2. Bridge claim-check (v1):   {"changeEventTimeStamp": "...",
//                                  "changeEventTypeName": "...",
//                                  "marketingPlanIdentifier": "...",
//                                  "hdfsPath": "/.../<eventId>.json",
//                                  "checksum": "<sha-256 of file bytes>",
//                                  "eventId": "<sha-256 of JMS message id>",
//                                  "source": "mq-kafka-bridge",
//                                  "schemaVersion": 1}
//
// The file at hdfsPath has EXACTLY the legacy inline shape, so resolving a
// claim-check message yields a document BluepcsPMMPLusProcessor already
// understands — no processor changes needed.
//
// Detection contract (in this order):
//   - schemaVersion present  -> bridge claim-check message
//       - schemaVersion == 1 -> fetch hdfsPath, verify checksum, use file content
//       - schemaVersion  > 1 -> unknown future contract: fail loudly (do not guess)
//   - schemaVersion absent   -> legacy Talend inline message: use raw value as-is
//       (belt-and-braces: a bridge build older than 8003daa lacks the markers but
//        also lacks RestAPIResponse — the hdfsPath fallback below covers it)
//
// Duplicate handling: the bridge is at-least-once, and offsets commit to HBase
// AFTER processing — so a redelivered claim-check message can reference a file
// this job already processed and archived out of the landing directory.
// resolveOrSkip() treats a missing HDFS file as such a duplicate: WARN + skip,
// instead of crash-looping the batch on FileNotFoundException.
// =============================================================================

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import java.io.FileNotFoundException
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

object BridgeMessageResolver extends Serializable {

  /** Highest claim-check contract version this consumer understands. */
  val SupportedSchemaVersion = 1

  val ExpectedSource = "mq-kafka-bridge"

  @transient private lazy val logger = LoggerFactory.getLogger(getClass)

  final case class ResolvedMessage(
      json: String,              // full wrapper document (legacy shape), ready for the processor
      isClaimCheck: Boolean,     // true when fetched via hdfsPath
      eventId: Option[String]    // present only for bridge messages — usable for dedupe
  )

  /**
   * Normalizes one Kafka record value; returns None for a claim-check duplicate
   * whose HDFS file no longer exists (already processed + archived).
   * Still throws on unknown schemaVersion or checksum mismatch — those are
   * contract violations that must stop the job, not be skipped.
   */
  def resolveOrSkip(raw: String, fs: FileSystem, mapper: ObjectMapper): Option[ResolvedMessage] =
    try {
      Some(resolve(raw, fs, mapper))
    } catch {
      case e: FileNotFoundException =>
        logger.warn(
          "@@@ Claim-check HDFS file missing — treating as already-processed duplicate and skipping: {}",
          e.getMessage)
        None
    }

  /** Strict variant: throws on any resolution problem, including a missing file. */
  def resolve(raw: String, fs: FileSystem, mapper: ObjectMapper): ResolvedMessage = {
    val node = mapper.readTree(raw)

    if (isBridgeMessage(node)) {
      val version = node.path("schemaVersion").asInt(1) // pre-marker builds -> treat as v1
      require(
        version <= SupportedSchemaVersion,
        s"Unsupported bridge schemaVersion $version " +
          s"(this consumer supports <= $SupportedSchemaVersion). " +
          s"eventId=${node.path("eventId").asText("?")} — update the consumer before processing."
      )
      resolveClaimCheck(node, fs)
    } else {
      // Legacy Talend inline message — pass through untouched.
      ResolvedMessage(raw, isClaimCheck = false, eventId = None)
    }
  }

  /**
   * Primary discriminator: schemaVersion. Fallback: hdfsPath-without-RestAPIResponse
   * covers messages from bridge builds that predate the marker fields.
   */
  private def isBridgeMessage(node: JsonNode): Boolean =
    node.hasNonNull("schemaVersion") ||
      (node.hasNonNull("hdfsPath") && node.hasNonNull("eventId") && !node.has("RestAPIResponse"))

  private def resolveClaimCheck(node: JsonNode, fs: FileSystem): ResolvedMessage = {
    val hdfsPath = node.get("hdfsPath").asText()
    val eventId  = node.get("eventId").asText()

    val bytes = {
      val in = fs.open(new Path(hdfsPath)) // throws FileNotFoundException if archived
      try org.apache.commons.io.IOUtils.toByteArray(in)
      finally in.close()
    }

    // Verify integrity against the checksum the bridge computed at write time.
    val actual = sha256Hex(bytes)
    val expected = node.path("checksum").asText("")
    require(
      expected.isEmpty || actual == expected,
      s"Checksum mismatch for $hdfsPath (eventId=$eventId): expected $expected, got $actual"
    )

    ResolvedMessage(
      json = new String(bytes, StandardCharsets.UTF_8),
      isClaimCheck = true,
      eventId = Some(eventId)
    )
  }

  private def sha256Hex(bytes: Array[Byte]): String =
    MessageDigest.getInstance("SHA-256").digest(bytes).map("%02x".format(_)).mkString
}

// =============================================================================
// Wiring into runKafkaMode (the actual change — replaces the processor call)
// =============================================================================
// Additional imports needed at the top of BluepcsPMMPLusConsumer:
//
//   import com.fasterxml.jackson.databind.ObjectMapper
//   import org.apache.hadoop.conf.Configuration
//   import org.apache.hadoop.fs.FileSystem
//
// Inside stream.foreachRDD, keep everything up to and including the
// debug-sampling `inputRDD` exactly as-is, then REPLACE:
//
//   BluepcsPMMPLusProcessor.processJSONMessages(
//     common_conf, bluepcs_conf, inputRDD, kafkaOffsetDetailsDF, env)
//
// WITH:
//
//   // Normalize both formats (legacy Talend inline vs bridge claim-check) to
//   // the same wrapper shape; (partition, offset, json) tuple layout unchanged,
//   // so the processor and HBase offset management need no modifications.
//   val resolvedRDD =
//     inputRDD.mapPartitions { it =>
//       // one FileSystem + ObjectMapper per partition, not per record
//       val fs = FileSystem.get(new Configuration())
//       val mapper = new ObjectMapper()
//       it.flatMap {
//         case (partition, offset, raw) =>
//           BridgeMessageResolver
//             .resolveOrSkip(raw, fs, mapper)
//             .map(resolved => (partition, offset, resolved.json))
//       }
//     }
//
//   logger.info("@@@ Messages normalized (claim-check resolved from HDFS where present)")
//
//   BluepcsPMMPLusProcessor.processJSONMessages(
//     common_conf, bluepcs_conf, resolvedRDD, kafkaOffsetDetailsDF, env)
//
// Behavior notes:
//   - Legacy messages: passed through byte-for-byte; the processor sees exactly
//     what it sees today.
//   - Bridge messages: replaced by the HDFS file content (same shape as
//     legacy); checksum-verified.
//   - Unknown schemaVersion / checksum mismatch: the batch fails and, because
//     offsets are committed to HBase only after processing, the messages are
//     reprocessed after the consumer is fixed — nothing is lost.
//   - Missing HDFS file: skipped as an already-processed duplicate (WARN log).
//     Offset bookkeeping is unaffected — the skip only drops the record from
//     the processing RDD, not from the offset ranges.
//   - Debug mode: sampling happens BEFORE resolution, so debug runs exercise
//     the HDFS fetch path too (on the sampled records only).
//   - Executors need HDFS read access to the bridge landing directory
//     (test: /test/oort/product/bluepcs/hive/csv) — same Kerberos login the
//     job already uses for Hive/HBase.
//   - After successful processing, this job owns the file lifecycle: move
//     processed files out of the landing dir (archive/), per the
//     flat-landing-directory convention.
// =============================================================================
