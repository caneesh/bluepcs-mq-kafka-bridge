package com.hcsc.bridge.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Primary
@Profile("!local")
public class SafeAuditPublisher implements AuditPublisher {

    private static final Logger logger = LoggerFactory.getLogger(SafeAuditPublisher.class);

    private final AuditPublisher delegate;
    private final boolean auditEnabled;

    /**
     * @param publisherMode where audit events go: {@code kafka} (default) publishes to the
     *        audit topic; {@code log} writes JSON lines to the "audit" logger instead — the
     *        bring-up fallback while the Kafka audit topic does not exist yet (a missing topic
     *        surfaces as TopicAuthorizationException on a secured cluster, and every event
     *        would be silently dropped).
     * @param auditEnabled master switch. Enforced HERE — it was previously bound and
     *        documented but read by nothing, so setting it false silently kept auditing.
     *        Turning it off blinds the reconciliation and balance checks, hence the
     *        startup warning.
     */
    public SafeAuditPublisher(
            @Qualifier("kafkaAuditPublisher") AuditPublisher kafkaPublisher,
            @Qualifier("loggingAuditPublisher") AuditPublisher loggingPublisher,
            @Value("${bridge.audit.publisher:kafka}") String publisherMode,
            @Value("${bridge.audit.enabled:true}") boolean auditEnabled) {
        this.auditEnabled = auditEnabled;
        if (!auditEnabled) {
            logger.warn("bridge.audit.enabled=false — NO audit events will be emitted. The audit "
                    + "trail, gap check (scripts/audit-gap-check.sh) and balance check "
                    + "(scripts/abc-balance-check.sh) are all blind while this is set.");
        }
        if ("log".equalsIgnoreCase(publisherMode.trim())) {
            this.delegate = loggingPublisher;
            logger.warn("Audit publisher mode is LOG: audit events are written to the 'audit' "
                    + "logger, NOT to the Kafka audit topic. Switch bridge.audit.publisher back "
                    + "to 'kafka' once the audit topic exists.");
        } else {
            this.delegate = kafkaPublisher;
            if (!"kafka".equalsIgnoreCase(publisherMode.trim())) {
                logger.warn("Unknown bridge.audit.publisher value '{}' — defaulting to 'kafka'",
                        publisherMode);
            }
        }
    }

    @Override
    public void publish(AuditEvent event) {
        if (!auditEnabled) {
            return;
        }
        try {
            delegate.publish(event);
        } catch (Exception e) {
            logger.error("Audit publish failed but continuing processing: {} - {}",
                    event.getEventType(), event.getMessageId(), e);
        }
    }

    @Override
    public void publishAsync(AuditEvent event) {
        if (!auditEnabled) {
            return;
        }
        try {
            delegate.publishAsync(event);
        } catch (Exception e) {
            logger.error("Async audit publish failed but continuing processing: {} - {}",
                    event.getEventType(), event.getMessageId(), e);
        }
    }
}
