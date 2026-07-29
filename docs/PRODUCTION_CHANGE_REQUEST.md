# Production Deployment — Change Request Details

***HCSC Non-Public – For Internal Use Only***

| Field | Value |
|-------|-------|
| APP ID | Product Data {PRD00000331} |
| Application | Health Analytics Product Gold {APP00007047} |
| SOURCE | Facet / Blue PCS / Datawarehouse |
| TARGET | Data Lake |
| DATAFLOW | Facet/Blue PCS/Datawarehouse – Data Lake |

## Business Justification

Deploy the BluePCS MQ–Kafka bridge, a Java service that reliably moves BluePCS
product/plan change events from IBM MQ into the Data Lake and notifies the
downstream Product Gold load via Kafka, replacing the legacy Talend job that
performs this transfer today.

## Description

**1. What are we doing?**

Replacing the legacy Talend job with a new bridge service that consumes BluePCS
product change events from IBM MQ, enriches them via the Marketing Plan API,
lands the payload as JSON in the Data Lake (HDFS), and publishes a claim-check
notification to Kafka for the downstream Product Gold load.

**2. How are we doing?**

A Spring Boot service running on the Hadoop edge node under Control-M
supervision, using at-least-once delivery (HDFS write → checksum verify → Kafka
publish → MQ acknowledge) with Kerberos/SSL security and a full audit-event
trail.

**3. Why are we doing?**

To retire the legacy Talend-based transfer and provide reliable, near-real-time
delivery of BluePCS product data into the Data Lake with no message loss,
end-to-end traceability, and automated monitoring/alerting.

**4. Who is the end user?**

Health Analytics / Product Gold data consumers — downstream analytics and
reporting teams querying the Product Gold tables in the Data Lake.

**5. How will the end user be benefitted with this release?**

They get timelier and more complete product data in the gold tables, with
checksum verification, deduplication by event ID, and an audit trail
guaranteeing nothing is silently lost.

**6. What would be the impact if this change were not to take place?**

BluePCS product changes would not flow into the Data Lake, leaving Product Gold
tables stale or incomplete for analytics and reporting.

**7. Who would be impacted?**

Health Analytics Product Gold consumers and any reporting/business users
relying on current BluePCS product data from the Data Lake.
