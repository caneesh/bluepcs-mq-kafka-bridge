package com.hcsc.bridge.mock;

import com.hcsc.bridge.core.ProcessingContext;
import com.hcsc.bridge.model.EnrichedPayload;
import com.hcsc.bridge.model.ParsedPayload;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class SamplePayloadGenerator {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    private final AtomicLong sequenceNumber = new AtomicLong(0);

    public ParsedPayload generateParsedPayload() {
        long seq = sequenceNumber.incrementAndGet();
        String messageId = "MSG-" + UUID.randomUUID().toString().substring(0, 8);
        return generateParsedPayload(messageId, seq);
    }

    public ParsedPayload generateParsedPayload(String messageId) {
        long seq = sequenceNumber.incrementAndGet();
        return generateParsedPayload(messageId, seq);
    }

    private ParsedPayload generateParsedPayload(String messageId, long seq) {
        String entityId = "PLAN-" + seq;
        String versionId = String.valueOf(seq);
        String transactionId = entityId + "-v" + versionId;
        String eventType = "ReadyToSell";
        String effectiveDate = "2026-01-01";

        Map<String, Object> planNotification =
                buildPlanNotification(entityId, versionId, effectiveDate, eventType);

        String rawPayload;
        try {
            Map<String, Object> raw = new HashMap<>();
            raw.put("planNotification", planNotification);
            rawPayload = OBJECT_MAPPER.writeValueAsString(raw);
        } catch (JsonProcessingException e) {
            rawPayload = "{}";
        }

        return new ParsedPayload(
                messageId,
                transactionId,
                eventType,
                entityId,
                effectiveDate,
                planNotification,
                Instant.now(),
                rawPayload
        );
    }

    private Map<String, Object> buildPlanNotification(String entityId, String versionId,
                                                      String effectiveDate, String eventType) {
        Map<String, Object> effectivityDates = new HashMap<>();
        effectivityDates.put("effectiveStartDate", effectiveDate + "T06:00:00.000Z");
        effectivityDates.put("retiredDate", "9999-12-31T00:00:00.000Z");
        effectivityDates.put("closedDate", "9999-12-31T00:00:00.000Z");

        Map<String, Object> planVersion = new HashMap<>();
        planVersion.put("planVersionIdentifier", versionId);
        planVersion.put("planEffectivityDates", effectivityDates);
        planVersion.put("planStatus", "Open");

        Map<String, Object> changeEvent = new HashMap<>();
        changeEvent.put("typeName", "Update");
        changeEvent.put("eventName", eventType);
        changeEvent.put("timestamp", Instant.now().toString());

        Map<String, Object> planNotification = new HashMap<>();
        planNotification.put("marketingPlanIdentifier", entityId);
        planNotification.put("lineOfBusiness", "Retail");
        planNotification.put("planVersion", planVersion);
        planNotification.put("changeEvent", changeEvent);
        planNotification.put("mockIndicator", "N");
        return planNotification;
    }

    public EnrichedPayload generateEnrichedPayload() {
        ParsedPayload parsed = generateParsedPayload();
        return generateEnrichedPayload(parsed);
    }

    public EnrichedPayload generateEnrichedPayload(ParsedPayload parsed) {
        Map<String, Object> additionalData = new HashMap<>();
        additionalData.put("enrichedAt", Instant.now().toString());
        additionalData.put("source", "sample-generator");
        additionalData.put("version", "1.0");

        String marketingPlanId = "MP-" + parsed.getTransactionId();
        String campaignId = "CAMP-" + parsed.getEntityId();

        String eventId = "event-id-" + parsed.getMessageId();
        ProcessingContext ctx = new ProcessingContext(eventId, parsed.getMessageId(), Instant.now());

        return new EnrichedPayload(
                parsed,
                ctx,
                additionalData,
                marketingPlanId,
                campaignId,
                Instant.now()
        );
    }

    public String generateHdfsPath(EnrichedPayload payload) {
        // Flat landing directory, matching HdfsSafePayloadWriter#buildTargetPath
        return String.format("/data/bridge/payloads/%s.json", payload.getEventId());
    }

    public Map<String, Object> generateSampleOrderData() {
        long seq = sequenceNumber.incrementAndGet();
        Map<String, Object> data = new HashMap<>();
        data.put("orderId", "ORD-" + seq);
        data.put("customerId", "CUST-" + (seq % 100));
        data.put("amount", 100.0 + (seq % 1000));
        data.put("currency", "USD");
        data.put("status", "CREATED");
        data.put("createdAt", Instant.now().toString());
        return data;
    }

    public void reset() {
        sequenceNumber.set(0);
    }
}
