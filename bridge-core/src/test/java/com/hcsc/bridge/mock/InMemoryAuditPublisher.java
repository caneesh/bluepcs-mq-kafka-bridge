package com.hcsc.bridge.mock;

import com.hcsc.bridge.audit.AuditEvent;
import com.hcsc.bridge.audit.AuditEventType;
import com.hcsc.bridge.audit.AuditPublisher;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

public class InMemoryAuditPublisher implements AuditPublisher {

    private final List<AuditEvent> events = new CopyOnWriteArrayList<>();

    @Override
    public void publish(AuditEvent event) {
        events.add(event);
    }

    @Override
    public void publishAsync(AuditEvent event) {
        events.add(event);
    }

    public List<AuditEvent> getEvents() {
        return new ArrayList<>(events);
    }

    public List<AuditEvent> getEventsByMessageId(String messageId) {
        return events.stream()
                .filter(e -> messageId.equals(e.getMessageId()))
                .collect(Collectors.toList());
    }

    public List<AuditEvent> getEventsByType(AuditEventType eventType) {
        return events.stream()
                .filter(e -> e.getEventType() == eventType)
                .collect(Collectors.toList());
    }

    public void clear() {
        events.clear();
    }

    public int size() {
        return events.size();
    }

    public boolean hasEventOfType(AuditEventType eventType) {
        return events.stream().anyMatch(e -> e.getEventType() == eventType);
    }
}
