package com.hcsc.bridge.audit;

public interface AuditPublisher {

    void publish(AuditEvent event);

    void publishAsync(AuditEvent event);
}
