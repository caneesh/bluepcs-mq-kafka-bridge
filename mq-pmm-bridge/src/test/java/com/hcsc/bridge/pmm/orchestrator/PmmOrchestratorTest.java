package com.hcsc.bridge.pmm.orchestrator;

import com.hcsc.bridge.audit.AuditEvent;
import com.hcsc.bridge.audit.AuditEventType;
import com.hcsc.bridge.core.EventIdGenerator;
import com.hcsc.bridge.hdfs.HdfsFileOperations;
import com.hcsc.bridge.hdfs.HdfsWriteException;
import com.hcsc.bridge.hdfs.SafeHdfsWriter;
import com.hcsc.bridge.mock.InMemoryAuditPublisher;
import com.hcsc.bridge.model.HdfsWriteResult;
import com.hcsc.bridge.model.MqMessage;
import com.hcsc.bridge.orchestrator.ProcessingResult;
import com.hcsc.bridge.pmm.api.PmmApiClient;
import com.hcsc.bridge.pmm.api.PmmApiException;
import com.hcsc.bridge.pmm.api.PmmApiResponse;
import com.hcsc.bridge.pmm.hdfs.WindowedPathResolver;
import com.hcsc.bridge.pmm.template.PmmRequestTemplate;
import com.hcsc.bridge.pmm.xml.PmmExtractedValues;
import com.hcsc.bridge.pmm.xml.PmmXmlException;
import com.hcsc.bridge.pmm.xml.PmmXmlExtractor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("PmmOrchestrator")
class PmmOrchestratorTest {

    private static final String PAYLOAD = "<PmmMessage/>";
    private static final Instant PUT_TIME = Instant.parse("2026-09-13T05:00:00Z");   // window 04
    private static final Instant RECEIVE_TIME = Instant.parse("2026-09-13T09:00:00Z"); // window 08

    @Mock private PmmXmlExtractor extractor;
    @Mock private PmmRequestTemplate template;
    @Mock private PmmApiClient apiClient;
    @Mock private SafeHdfsWriter hdfsWriter;
    @Mock private HdfsFileOperations hdfsFileOperations;

    private InMemoryAuditPublisher audit;
    private EventIdGenerator eventIdGenerator;
    private WindowedPathResolver resolver;
    private PmmOrchestrator orchestrator;

    @BeforeEach
    void setUp() {
        audit = new InMemoryAuditPublisher();
        eventIdGenerator = new EventIdGenerator();
        resolver = new WindowedPathResolver("/data/pmm", "", 4, "UTC", "yyyy-MM-dd", ".xml",
                Clock.fixed(RECEIVE_TIME, ZoneOffset.UTC));
        orchestrator = new PmmOrchestrator(extractor, template, apiClient, hdfsWriter, hdfsFileOperations,
                resolver, eventIdGenerator, audit, true);
    }

    private MqMessage message(String id, Instant jmsTimestamp) {
        return new MqMessage(id, "corr", PAYLOAD, RECEIVE_TIME, "PMM.Q", jmsTimestamp);
    }

    private String eventIdOf(String messageId) {
        return eventIdGenerator.generateEventId(messageId);
    }

    /** Stubs everything up to and including the API call; the writer is stubbed per test. */
    private void stubUpToApi() throws IOException {
        when(extractor.extract(PAYLOAD, "MSG-1")).thenReturn(new PmmExtractedValues("a", "b"));
        when(hdfsFileOperations.exists(anyString())).thenReturn(false);
        when(template.render(any())).thenReturn("<req/>");
        when(apiClient.submit("<req/>", eventIdOf("MSG-1"))).thenReturn(new PmmApiResponse(200, "<resp/>", 12L));
    }

    private void stubHappyPath() throws IOException {
        stubUpToApi();
        when(hdfsWriter.write(anyString(), eq("<resp/>"), eq("MSG-1")))
                .thenAnswer(inv -> HdfsWriteResult.success(inv.getArgument(0), "sum", 7));
    }

    private List<AuditEventType> eventTypes() {
        return audit.getEvents().stream().map(AuditEvent::getEventType).collect(Collectors.toList());
    }

    @Nested
    @DisplayName("success path")
    class SuccessPath {

        @Test
        @DisplayName("extracts, renders, posts, lands the raw response and audits every stage with pipeline=pmm")
        void fullFlow() throws IOException {
            stubHappyPath();

            ProcessingResult result = orchestrator.process(message("MSG-1", PUT_TIME));

            assertThat(result.isSuccessful()).isTrue();
            assertThat(result.getEventId()).isEqualTo(eventIdOf("MSG-1"));
            assertThat(result.getHdfsPath()).isEqualTo("/data/pmm/2026-09-13/04/" + eventIdOf("MSG-1") + ".xml");
            assertThat(result.getKafkaOffset()).isNull();
            assertThat(eventTypes()).containsExactly(
                    AuditEventType.MESSAGE_RECEIVED, AuditEventType.MESSAGE_PARSED,
                    AuditEventType.API_CALL_COMPLETED, AuditEventType.HDFS_WRITE_COMPLETED,
                    AuditEventType.PROCESSING_COMPLETED);
            assertThat(audit.getEvents()).allSatisfy(e ->
                    assertThat(e.getMetadata()).containsEntry("pipeline", "pmm"));
            assertThat(audit.getEventsByType(AuditEventType.MESSAGE_RECEIVED).get(0).getMetadata())
                    .containsEntry("anchorSource", "jmsTimestamp").containsEntry("window", "2026-09-13/04");
            assertThat(audit.getEventsByType(AuditEventType.API_CALL_COMPLETED).get(0).getMetadata())
                    .containsEntry("statusCode", 200).containsEntry("durationMs", 12L);
        }

        @Test
        @DisplayName("anchors the window on the receive time when the message has no JMSTimestamp")
        void receivedAtFallback() throws IOException {
            stubHappyPath();

            ProcessingResult result = orchestrator.process(message("MSG-1", null));

            assertThat(result.getHdfsPath()).contains("/2026-09-13/08/");
            assertThat(audit.getEventsByType(AuditEventType.MESSAGE_RECEIVED).get(0).getMetadata())
                    .containsEntry("anchorSource", "receivedAt");
        }

        @Test
        @DisplayName("derives the eventId from the payload hash when the JMS message id is missing")
        void payloadHashFallback() throws IOException {
            when(extractor.extract(eq(PAYLOAD), any())).thenReturn(new PmmExtractedValues("a", "b"));
            when(hdfsFileOperations.exists(anyString())).thenReturn(false);
            when(template.render(any())).thenReturn("<req/>");
            when(apiClient.submit(anyString(), anyString())).thenReturn(new PmmApiResponse(200, "<resp/>", 1L));
            when(hdfsWriter.write(anyString(), anyString(), any()))
                    .thenAnswer(inv -> HdfsWriteResult.success(inv.getArgument(0), "sum", 7));

            ProcessingResult result = orchestrator.process(message(null, PUT_TIME));

            assertThat(result.isSuccessful()).isTrue();
            assertThat(result.getEventId()).isEqualTo(eventIdOf(PAYLOAD));
        }

        @Test
        @DisplayName("reports HDFS_WRITE_SKIPPED when the writer found identical bytes already in place")
        void writerAlreadyExists() throws IOException {
            stubUpToApi();
            when(hdfsWriter.write(anyString(), eq("<resp/>"), eq("MSG-1")))
                    .thenAnswer(inv -> HdfsWriteResult.alreadyExists(inv.getArgument(0), "sum"));

            ProcessingResult result = orchestrator.process(message("MSG-1", PUT_TIME));

            assertThat(result.isSuccessful()).isTrue();
            assertThat(eventTypes()).contains(AuditEventType.HDFS_WRITE_SKIPPED)
                    .doesNotContain(AuditEventType.HDFS_WRITE_COMPLETED);
        }
    }

    @Nested
    @DisplayName("redelivery pre-check")
    class Redelivery {

        @Test
        @DisplayName("skips the web-service call entirely when the target file already exists")
        void skipsApiWhenTargetExists() throws IOException {
            when(extractor.extract(PAYLOAD, "MSG-1")).thenReturn(new PmmExtractedValues("a", "b"));
            String target = resolver.resolve(eventIdOf("MSG-1"), PUT_TIME);
            when(hdfsFileOperations.exists(target)).thenReturn(true);

            ProcessingResult result = orchestrator.process(message("MSG-1", PUT_TIME));

            assertThat(result.isSuccessful()).isTrue();
            assertThat(result.getHdfsPath()).isEqualTo(target);
            verify(apiClient, never()).submit(anyString(), anyString());
            verify(hdfsWriter, never()).write(anyString(), anyString(), any());
            assertThat(eventTypes()).containsExactly(AuditEventType.MESSAGE_RECEIVED, AuditEventType.MESSAGE_PARSED,
                    AuditEventType.HDFS_WRITE_SKIPPED, AuditEventType.PROCESSING_COMPLETED);
            assertThat(audit.getEventsByType(AuditEventType.HDFS_WRITE_SKIPPED).get(0).getMetadata())
                    .containsEntry("reason", "target-exists-before-api-call");
        }

        @Test
        @DisplayName("calls the web service when the pre-check is disabled")
        void preCheckDisabled() throws IOException {
            orchestrator = new PmmOrchestrator(extractor, template, apiClient, hdfsWriter, hdfsFileOperations,
                    resolver, eventIdGenerator, audit, false);
            when(extractor.extract(PAYLOAD, "MSG-1")).thenReturn(new PmmExtractedValues("a", "b"));
            when(template.render(any())).thenReturn("<req/>");
            when(apiClient.submit("<req/>", eventIdOf("MSG-1"))).thenReturn(new PmmApiResponse(200, "<resp/>", 1L));
            when(hdfsWriter.write(anyString(), eq("<resp/>"), eq("MSG-1")))
                    .thenAnswer(inv -> HdfsWriteResult.success(inv.getArgument(0), "sum", 7));

            orchestrator.process(message("MSG-1", PUT_TIME));

            verify(hdfsFileOperations, never()).exists(anyString());
            verify(apiClient).submit("<req/>", eventIdOf("MSG-1"));
        }

        @Test
        @DisplayName("treats a failing existence check as an HDFS failure (no ack)")
        void existsFails() throws IOException {
            when(extractor.extract(PAYLOAD, "MSG-1")).thenReturn(new PmmExtractedValues("a", "b"));
            when(hdfsFileOperations.exists(anyString())).thenThrow(new IOException("namenode down"));

            ProcessingResult result = orchestrator.process(message("MSG-1", PUT_TIME));

            assertThat(result.isFailed()).isTrue();
            assertThat(result.getErrorCode()).isEqualTo("HDFS_ERROR");
            verify(apiClient, never()).submit(anyString(), anyString());
            assertThat(eventTypes()).contains(AuditEventType.HDFS_WRITE_FAILED);
        }
    }

    @Nested
    @DisplayName("failure dispositions")
    class Failures {

        @Test
        @DisplayName("parse failure quarantines the raw payload to errors/<eventId>.xml and returns QUARANTINED")
        void parseFailureQuarantines() {
            when(extractor.extract(PAYLOAD, "MSG-1")).thenThrow(new PmmXmlException("bad xml", "MSG-1"));
            String quarantine = "/data/pmm/errors/" + eventIdOf("MSG-1") + ".xml";
            when(hdfsWriter.write(quarantine, PAYLOAD, "MSG-1")).thenReturn(HdfsWriteResult.success(quarantine, "s", 1));

            ProcessingResult result = orchestrator.process(message("MSG-1", PUT_TIME));

            assertThat(result.isQuarantined()).isTrue();
            assertThat(result.getErrorCode()).isEqualTo("PARSE_ERROR");
            assertThat(result.getHdfsPath()).isEqualTo(quarantine);
            AuditEvent q = audit.getEventsByType(AuditEventType.MESSAGE_QUARANTINED).get(0);
            assertThat(q.getMetadata()).containsEntry("errorCode", "PARSE_ERROR")
                    .containsEntry("hdfsPath", quarantine).containsEntry("pipeline", "pmm");
            verify(apiClient, never()).submit(anyString(), anyString());
        }

        @Test
        @DisplayName("parse failure with a failing quarantine write returns FAILURE so MQ redelivers")
        void parseFailureQuarantineFails() {
            when(extractor.extract(PAYLOAD, "MSG-1")).thenThrow(new PmmXmlException("bad xml", "MSG-1"));
            when(hdfsWriter.write(anyString(), anyString(), any()))
                    .thenThrow(new HdfsWriteException("hdfs down", "/x", "MSG-1"));

            ProcessingResult result = orchestrator.process(message("MSG-1", PUT_TIME));

            assertThat(result.isFailed()).isTrue();
            assertThat(result.getErrorCode()).isEqualTo("PARSE_ERROR");
            assertThat(eventTypes()).contains(AuditEventType.PROCESSING_FAILED)
                    .doesNotContain(AuditEventType.MESSAGE_QUARANTINED);
        }

        @Test
        @DisplayName("retryable API failure audits API_CALL_FAILED and returns FAILURE without quarantining")
        void retryableApiFailure() throws IOException {
            when(extractor.extract(PAYLOAD, "MSG-1")).thenReturn(new PmmExtractedValues("a", "b"));
            when(hdfsFileOperations.exists(anyString())).thenReturn(false);
            when(template.render(any())).thenReturn("<req/>");
            when(apiClient.submit(anyString(), anyString()))
                    .thenThrow(new PmmApiException("503", eventIdOf("MSG-1"), 503, true));

            ProcessingResult result = orchestrator.process(message("MSG-1", PUT_TIME));

            assertThat(result.isFailed()).isTrue();
            assertThat(result.getErrorCode()).isEqualTo("API_ERROR");
            assertThat(eventTypes()).contains(AuditEventType.API_CALL_FAILED)
                    .doesNotContain(AuditEventType.MESSAGE_QUARANTINED);
            verify(hdfsWriter, never()).write(anyString(), anyString(), any());
        }

        @Test
        @DisplayName("permanent API failure audits API_CALL_FAILED and quarantines with errorCode=API_ERROR")
        void permanentApiFailure() throws IOException {
            when(extractor.extract(PAYLOAD, "MSG-1")).thenReturn(new PmmExtractedValues("a", "b"));
            when(hdfsFileOperations.exists(anyString())).thenReturn(false);
            when(template.render(any())).thenReturn("<req/>");
            when(apiClient.submit(anyString(), anyString()))
                    .thenThrow(new PmmApiException("404", eventIdOf("MSG-1"), 404, false));
            String quarantine = resolver.quarantinePath(eventIdOf("MSG-1"));
            when(hdfsWriter.write(quarantine, PAYLOAD, "MSG-1")).thenReturn(HdfsWriteResult.success(quarantine, "s", 1));

            ProcessingResult result = orchestrator.process(message("MSG-1", PUT_TIME));

            assertThat(result.isQuarantined()).isTrue();
            assertThat(result.getErrorCode()).isEqualTo("API_ERROR");
            assertThat(eventTypes()).containsExactly(AuditEventType.MESSAGE_RECEIVED, AuditEventType.MESSAGE_PARSED,
                    AuditEventType.API_CALL_FAILED, AuditEventType.MESSAGE_QUARANTINED);
            assertThat(audit.getEventsByType(AuditEventType.MESSAGE_QUARANTINED).get(0).getMetadata())
                    .containsEntry("errorCode", "API_ERROR");
        }

        @Test
        @DisplayName("permanent API failure with a failing quarantine write returns FAILURE")
        void permanentApiFailureQuarantineFails() throws IOException {
            when(extractor.extract(PAYLOAD, "MSG-1")).thenReturn(new PmmExtractedValues("a", "b"));
            when(hdfsFileOperations.exists(anyString())).thenReturn(false);
            when(template.render(any())).thenReturn("<req/>");
            when(apiClient.submit(anyString(), anyString()))
                    .thenThrow(new PmmApiException("400", eventIdOf("MSG-1"), 400, false));
            when(hdfsWriter.write(anyString(), anyString(), any()))
                    .thenThrow(new HdfsWriteException("hdfs down", "/x", "MSG-1"));

            ProcessingResult result = orchestrator.process(message("MSG-1", PUT_TIME));

            assertThat(result.isFailed()).isTrue();
            assertThat(result.getErrorCode()).isEqualTo("API_ERROR");
        }

        @Test
        @DisplayName("HDFS write failure audits HDFS_WRITE_FAILED and returns FAILURE")
        void hdfsFailure() throws IOException {
            when(extractor.extract(PAYLOAD, "MSG-1")).thenReturn(new PmmExtractedValues("a", "b"));
            when(hdfsFileOperations.exists(anyString())).thenReturn(false);
            when(template.render(any())).thenReturn("<req/>");
            when(apiClient.submit(anyString(), anyString())).thenReturn(new PmmApiResponse(200, "<resp/>", 1L));
            when(hdfsWriter.write(anyString(), anyString(), any()))
                    .thenThrow(new HdfsWriteException("checksum mismatch", "/x", "MSG-1"));

            ProcessingResult result = orchestrator.process(message("MSG-1", PUT_TIME));

            assertThat(result.isFailed()).isTrue();
            assertThat(result.getErrorCode()).isEqualTo("HDFS_ERROR");
            assertThat(eventTypes()).contains(AuditEventType.API_CALL_COMPLETED, AuditEventType.HDFS_WRITE_FAILED);
        }

        @Test
        @DisplayName("an unexpected RuntimeException is audited as PROCESSING_FAILED / UNEXPECTED_ERROR")
        void unexpectedFailure() throws IOException {
            when(extractor.extract(PAYLOAD, "MSG-1")).thenReturn(new PmmExtractedValues("a", "b"));
            when(hdfsFileOperations.exists(anyString())).thenReturn(false);
            when(template.render(any())).thenThrow(new IllegalStateException("boom"));

            ProcessingResult result = orchestrator.process(message("MSG-1", PUT_TIME));

            assertThat(result.isFailed()).isTrue();
            assertThat(result.getErrorCode()).isEqualTo("UNEXPECTED_ERROR");
            assertThat(eventTypes()).contains(AuditEventType.PROCESSING_FAILED);
        }
    }
}
