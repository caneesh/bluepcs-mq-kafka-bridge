package com.enterprise.bridge.mock;

import com.enterprise.bridge.core.ProcessingContext;
import com.enterprise.bridge.model.EnrichedPayload;
import com.enterprise.bridge.model.ParsedPayload;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
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
        String transactionId = "TXN-" + seq;
        String eventType = "order_created";
        String entityId = "ENTITY-" + seq;

        Map<String, Object> data = new HashMap<>();
        data.put("orderId", "ORD-" + seq);
        data.put("customerId", "CUST-" + (seq % 100));
        data.put("amount", 100.0 + (seq % 1000));
        data.put("currency", "USD");
        data.put("items", 1 + (int)(seq % 10));

        String rawPayload;
        try {
            Map<String, Object> raw = new HashMap<>();
            raw.put("messageId", messageId);
            raw.put("transactionId", transactionId);
            raw.put("eventType", eventType);
            raw.put("entityId", entityId);
            raw.put("data", data);
            raw.put("eventTimestamp", Instant.now().toString());
            rawPayload = OBJECT_MAPPER.writeValueAsString(raw);
        } catch (JsonProcessingException e) {
            rawPayload = "{}";
        }

        return new ParsedPayload(
                messageId,
                transactionId,
                eventType,
                entityId,
                data,
                Instant.now(),
                rawPayload
        );
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
        String datePrefix = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy/MM/dd"));
        return String.format("/data/bridge/payloads/%s/%s/%s.json",
                payload.getEventType(),
                datePrefix,
                payload.getEventId());
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
