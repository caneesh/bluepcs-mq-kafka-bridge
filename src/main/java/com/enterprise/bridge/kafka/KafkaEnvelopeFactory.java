package com.enterprise.bridge.kafka;

import com.enterprise.bridge.model.EnrichedPayload;
import com.enterprise.bridge.model.HdfsWriteResult;
import com.enterprise.bridge.model.KafkaEnvelope;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class KafkaEnvelopeFactory {

    private static final String SCHEMA_VERSION = "1.0";

    public KafkaEnvelope createEnvelope(EnrichedPayload payload, HdfsWriteResult hdfsResult) {
        return KafkaEnvelope.builder()
                .eventId(payload.getEventId())
                .bridgeMessageId(payload.getBridgeMessageId())
                .originalMqMessageId(payload.getOriginalMqMessageId())
                .messageId(payload.getMessageId())
                .transactionId(payload.getTransactionId())
                .eventType(payload.getEventType())
                .entityId(payload.getEntityId())
                .hdfsPath(hdfsResult.getHdfsPath())
                .checksum(hdfsResult.getChecksum())
                .marketingPlanId(payload.getMarketingPlanId())
                .campaignId(payload.getCampaignId())
                .eventTimestamp(payload.getParsedPayload().getEventTimestamp())
                .processedAt(Instant.now())
                .schemaVersion(SCHEMA_VERSION)
                .build();
    }
}
