# audit-hive-consumer

Spark Streaming job that consumes the MQ-Kafka bridge's **audit topic** and appends the
events to the Hive audit table `bluepcs.bridge_audit_event` — the queryable end-to-end
trail (MQ → bridge → Kafka → Hive product load) described in
[`../docs/AUDIT.md`](../docs/AUDIT.md).

Standalone Maven project — **not** part of the bridge's build. The bridge's office
build/transfer process is unaffected by this directory.

## Build

```bash
cd audit-hive-consumer
mvn clean package          # -> target/audit-hive-consumer-1.0.0.jar (shaded)
```

**Before building for the office cluster**: set `scala.version`/`scala.binary.version`/
`spark.version` in `pom.xml` to exactly what the existing `BluepcsPMMPLusConsumer` job
builds against, then rebuild. A Scala binary mismatch (2.11 vs 2.12) fails at *runtime*,
not compile time. If the office build uses the offline-repository strategy, run
`mvn -f audit-hive-consumer/pom.xml dependency:go-offline` at home first — this project's
dependency tree (Spark, Scala) is large and disjoint from the bridge's.

## Deploy

1. Create the Hive table once: [`hive/bridge_audit_event.ddl`](hive/bridge_audit_event.ddl)
2. Request Kafka ACLs: consume on the audit topic + group `bluepcs-audit-hive-consumer`
   for the job's principal
3. Submit — full example and configuration reference in the header of
   [`src/main/scala/com/hcsc/bluepcs/consumer/AuditHiveConsumer.scala`](src/main/scala/com/hcsc/bluepcs/consumer/AuditHiveConsumer.scala)
   (copy the Kafka JAAS/keytab confs from the existing consumer job's launcher)

Key confs (all `spark.bluepcs.audit.*`): `bootstrap.servers`, `topic`, `group.id`,
`hive.table`, `truststore`, `truststore.password`, `batch.seconds` (default 300).

## Operate

- At-least-once: offsets commit only after the Hive write; dedupe queries on
  `audit_event_id`. Consumer lag tolerance is hours — nothing pages on this job.
- `@@@ AUDIT->HIVE batch written` in the driver log is the per-batch liveness marker.
- Useful queries (latest state per message, redelivery-loop detection, parse failures)
  are in the Scala file's footer; end-to-end gap alerting is the bridge repo's
  `scripts/audit-gap-check.sh` (Control-M).
