package com.hcsc.bridge.audit;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("LoggingAuditPublisher")
class LoggingAuditPublisherTest {

    private Logger auditLogger;
    private ListAppender<ILoggingEvent> appender;

    @BeforeEach
    void setUp() {
        auditLogger = (Logger) LoggerFactory.getLogger("audit");
        appender = new ListAppender<>();
        appender.start();
        auditLogger.addAppender(appender);
    }

    @AfterEach
    void tearDown() {
        auditLogger.detachAppender(appender);
    }

    @Test
    @DisplayName("wire contract: logged JSON line carries timestamp as an ISO-8601 STRING")
    void shouldLogTimestampAsIsoString() throws Exception {
        AuditEvent event = AuditEvent.builder()
                .auditEventId("AUD-LOG-001")
                .eventId("event-log-001")
                .eventType(AuditEventType.MESSAGE_RECEIVED)
                .timestamp(Instant.now())
                .build();

        new LoggingAuditPublisher().publish(event);

        assertThat(appender.list).hasSize(1);
        // The log fallback must stay format-identical to the Kafka wire format: the Hive
        // consumer's Instant.parse and the gap check's lexicographic cutoff depend on it
        JsonNode json = new ObjectMapper().readTree(appender.list.get(0).getFormattedMessage());
        assertThat(json.get("timestamp").isTextual())
                .as("timestamp must serialize as a JSON string, not a number")
                .isTrue();
        assertThat(Instant.parse(json.get("timestamp").asText())).isBeforeOrEqualTo(Instant.now());
        assertThat(json.get("auditEventId").asText()).isEqualTo("AUD-LOG-001");
    }
}
