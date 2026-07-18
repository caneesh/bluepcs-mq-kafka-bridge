package com.hcsc.bluepcs.consumer

// =============================================================================
// BridgeMessageResolver — drop-in for BluepcsPMMPLusConsumer (Spark/Scala)
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
// claim-check message yields a document the existing processor already
// understands. Resolve first, then feed everything into the unchanged
// BluepcsPMMPLusProcessor path.
//
// Detection contract (in this order):
//   - schemaVersion present  -> bridge claim-check message
//       - schemaVersion == 1 -> fetch hdfsPath, verify checksum, use file content
//       - schemaVersion  > 1 -> unknown future contract: fail loudly (do not guess)
//   - schemaVersion absent   -> legacy Talend inline message: use raw value as-is
//       (belt-and-braces: a bridge build older than 8003daa lacks the markers but
//        also lacks RestAPIResponse — the hdfsPath fallback below covers it)
// =============================================================================

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

object BridgeMessageResolver extends Serializable {

  /** Highest claim-check contract version this consumer understands. */
  val SupportedSchemaVersion = 1

  val ExpectedSource = "mq-kafka-bridge"

  final case class ResolvedMessage(
      json: String,              // full wrapper document (legacy shape), ready for the processor
      isClaimCheck: Boolean,     // true when fetched via hdfsPath
      eventId: Option[String]    // present only for bridge messages — use for dedupe
  )

  /**
   * Normalizes one Kafka record value to the full wrapper document.
   * Throws on unknown schemaVersion or checksum mismatch — better a loud failure
   * than silently processing a contract we do not understand.
   */
  def resolve(raw: String, fs: FileSystem, mapper: ObjectMapper): ResolvedMessage = {
    val node = mapper.readTree(raw)

    if (isBridgeMessage(node)) {
      val version = node.get("schemaVersion").asInt()
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
      val in = fs.open(new Path(hdfsPath))
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

  // ---------------------------------------------------------------------------
  // Wiring into runKafkaMode — put this right after CAST(value AS STRING) and
  // before the existing parsing/processing:
  //
  //   import spark.implicits._
  //
  //   val resolved: Dataset[BridgeMessageResolver.ResolvedMessage] =
  //     kafkaDf
  //       .selectExpr("CAST(value AS STRING) AS raw")
  //       .as[String]
  //       .mapPartitions { it =>
  //         // one FileSystem + ObjectMapper per partition, not per record
  //         val fs = FileSystem.get(new Configuration())
  //         val mapper = new ObjectMapper()
  //         it.map(raw => BridgeMessageResolver.resolve(raw, fs, mapper))
  //       }
  //
  //   // Dedupe: at-least-once delivery means bridge messages can repeat.
  //   // Only bridge messages carry an eventId; legacy ones pass through.
  //   val deduped = resolved
  //     .withColumn("dedupeKey", coalesce($"eventId", sha2($"json", 256)))
  //     .dropDuplicates("dedupeKey")
  //
  //   // `json` column now ALWAYS holds the legacy wrapper shape:
  //   //   {"changeEventTimeStamp", "RestAPIResponse": {...}, "changeEventTypeName"}
  //   // -> feed into the existing BluepcsPMMPLusProcessor unchanged.
  //
  // Notes:
  //   - Spark executors need HDFS read access to the bridge landing directory
  //     (test: /test/oort/product/bluepcs/hive/csv) — same Kerberos login the
  //     job already uses for Hive.
  //   - After successful processing, the consumer owns the file lifecycle:
  //     move processed files out of the landing dir (archive/), per the
  //     flat-landing-directory convention.
  //   - Keep the resolver after Talend is decommissioned: it also guards
  //     against stray/manual test messages on the topic.
  // ---------------------------------------------------------------------------
}
