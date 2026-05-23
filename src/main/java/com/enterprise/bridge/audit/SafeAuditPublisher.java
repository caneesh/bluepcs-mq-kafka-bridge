package com.enterprise.bridge.audit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Primary;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

@Component
@Primary
@Profile("!local")
public class SafeAuditPublisher implements AuditPublisher {

    private static final Logger logger = LoggerFactory.getLogger(SafeAuditPublisher.class);

    private final AuditPublisher delegate;

    public SafeAuditPublisher(@Qualifier("kafkaAuditPublisher") AuditPublisher delegate) {
        this.delegate = delegate;
    }

    @Override
    public void publish(AuditEvent event) {
        try {
            delegate.publish(event);
        } catch (Exception e) {
            logger.error("Audit publish failed but continuing processing: {} - {}",
                    event.getEventType(), event.getMessageId(), e);
        }
    }

    @Override
    public void publishAsync(AuditEvent event) {
        try {
            delegate.publishAsync(event);
        } catch (Exception e) {
            logger.error("Async audit publish failed but continuing processing: {} - {}",
                    event.getEventType(), event.getMessageId(), e);
        }
    }
}
