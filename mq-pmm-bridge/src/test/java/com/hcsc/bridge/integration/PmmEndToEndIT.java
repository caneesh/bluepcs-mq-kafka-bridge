package com.hcsc.bridge.integration;

import com.hcsc.bridge.audit.AuditEventType;
import com.hcsc.bridge.core.EventIdGenerator;
import com.hcsc.bridge.hdfs.SafeHdfsWriter;
import com.hcsc.bridge.mock.InMemoryAuditPublisher;
import com.hcsc.bridge.mock.LocalFileSystemHdfsOperations;
import com.hcsc.bridge.mock.MockJwtTokenProvider;
import com.hcsc.bridge.model.MqMessage;
import com.hcsc.bridge.orchestrator.ProcessingResult;
import com.hcsc.bridge.pmm.api.RestPmmApiClient;
import com.hcsc.bridge.pmm.hdfs.WindowedPathResolver;
import com.hcsc.bridge.pmm.orchestrator.PmmOrchestrator;
import com.hcsc.bridge.pmm.template.PmmRequestTemplate;
import com.hcsc.bridge.pmm.xml.PmmXmlExtractor;
import okhttp3.OkHttpClient;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Whole PMM pipeline over a temp-directory HDFS fake and a MockWebServer: real extractor,
 * template, path resolver, safe writer, REST client and orchestrator.
 */
class PmmEndToEndIT {

    private static final String BASE = "/data/pmm";
    private static final Instant PUT_TIME = Instant.parse("2026-09-13T05:00:00Z");
    private static final String MESSAGE = "<PmmMessage><Product><ProductIdentifier>PRD-9</ProductIdentifier>"
            + "<EffectiveDate>2026-09-13</EffectiveDate></Product></PmmMessage>";
    private static final String RESPONSE = "<PmmResponse><Status>OK</Status><Stamp>2026-09-13T05:00:01Z</Stamp></PmmResponse>";

    private Path tempDir;
    private LocalFileSystemHdfsOperations hdfs;
    private MockWebServer server;
    private InMemoryAuditPublisher audit;
    private EventIdGenerator eventIdGenerator;
    private PmmOrchestrator orchestrator;

    @BeforeEach
    void setUp() throws IOException {
        tempDir = Files.createTempDirectory("pmm-e2e");
        hdfs = new LocalFileSystemHdfsOperations(tempDir);
        server = new MockWebServer();
        server.start();
        audit = new InMemoryAuditPublisher();
        eventIdGenerator = new EventIdGenerator();

        PmmXmlExtractor extractor = new PmmXmlExtractor("/PmmMessage/Product/ProductIdentifier",
                "/PmmMessage/Product/EffectiveDate", false, false);
        PmmRequestTemplate template = new PmmRequestTemplate(
                "<Req><Id>${value1}</Id><Date>${value2}</Date></Req>", "inline", true);
        MockJwtTokenProvider tokens = new MockJwtTokenProvider();
        tokens.setToken("tok");
        RestPmmApiClient apiClient = new RestPmmApiClient(tokens, server.url("/pmm").toString(), "cid", "sec",
                new OkHttpClient.Builder().readTimeout(2, TimeUnit.SECONDS).build(),
                2, 10, "application/xml", "application/xml");
        WindowedPathResolver resolver = new WindowedPathResolver(BASE, "", 4, "UTC", "yyyy-MM-dd", ".xml",
                Clock.fixed(PUT_TIME, ZoneOffset.UTC));
        orchestrator = new PmmOrchestrator(extractor, template, apiClient, new SafeHdfsWriter(hdfs, ".tmp"),
                hdfs, resolver, eventIdGenerator, audit, true);
    }

    @AfterEach
    void tearDown() throws IOException {
        server.shutdown();
        hdfs.cleanup();
    }

    private MqMessage message(String id, String payload) {
        return new MqMessage(id, null, payload, Instant.now(), "PMM.Q", PUT_TIME);
    }

    @Nested
    @DisplayName("happy path")
    class HappyPath {

        @Test
        @DisplayName("lands the raw response at <base>/<date>/<HH>/<eventId>.xml and posts the rendered request")
        void landsResponse() throws Exception {
            server.enqueue(new MockResponse().setBody(RESPONSE));

            ProcessingResult result = orchestrator.process(message("MSG-1", MESSAGE));

            String eventId = eventIdGenerator.generateEventId("MSG-1");
            String expected = BASE + "/2026-09-13/04/" + eventId + ".xml";
            assertThat(result.isSuccessful()).isTrue();
            assertThat(result.getHdfsPath()).isEqualTo(expected);
            assertThat(hdfs.readFile(expected)).isEqualTo(RESPONSE);
            assertThat(hdfs.listFiles(BASE + "/2026-09-13/04")).hasSize(1);
            RecordedRequest request = server.takeRequest();
            assertThat(request.getBody().readUtf8()).isEqualTo("<Req><Id>PRD-9</Id><Date>2026-09-13</Date></Req>");
            assertThat(request.getHeader("Authorization")).isEqualTo("Bearer tok");
            assertThat(audit.hasEventOfType(AuditEventType.API_CALL_COMPLETED)).isTrue();
            assertThat(audit.hasEventOfType(AuditEventType.HDFS_WRITE_COMPLETED)).isTrue();
            assertThat(audit.hasEventOfType(AuditEventType.PROCESSING_COMPLETED)).isTrue();
        }

        @Test
        @DisplayName("a redelivery finds the file and makes no second web-service call")
        void redeliverySkipsApi() {
            server.enqueue(new MockResponse().setBody(RESPONSE));
            server.enqueue(new MockResponse().setBody("<PmmResponse><Stamp>different</Stamp></PmmResponse>"));

            ProcessingResult first = orchestrator.process(message("MSG-1", MESSAGE));
            audit.clear();
            ProcessingResult second = orchestrator.process(message("MSG-1", MESSAGE));

            assertThat(first.isSuccessful()).isTrue();
            assertThat(second.isSuccessful()).isTrue();
            assertThat(second.getHdfsPath()).isEqualTo(first.getHdfsPath());
            assertThat(server.getRequestCount()).isEqualTo(1);
            assertThat(audit.hasEventOfType(AuditEventType.HDFS_WRITE_SKIPPED)).isTrue();
            assertThat(audit.hasEventOfType(AuditEventType.API_CALL_COMPLETED)).isFalse();
        }
    }

    @Nested
    @DisplayName("failure paths")
    class FailurePaths {

        @Test
        @DisplayName("quarantines an unparseable message at <base>/errors/<eventId>.xml")
        void quarantinesBadXml() throws Exception {
            ProcessingResult result = orchestrator.process(message("MSG-2", "<PmmMessage><Product>"));

            String quarantine = BASE + "/errors/" + eventIdGenerator.generateEventId("MSG-2") + ".xml";
            assertThat(result.isQuarantined()).isTrue();
            assertThat(result.getErrorCode()).isEqualTo("PARSE_ERROR");
            assertThat(hdfs.readFile(quarantine)).isEqualTo("<PmmMessage><Product>");
            assertThat(server.getRequestCount()).isZero();
        }

        @Test
        @DisplayName("quarantines on a permanent API rejection")
        void quarantinesOn404() throws Exception {
            server.enqueue(new MockResponse().setResponseCode(404));

            ProcessingResult result = orchestrator.process(message("MSG-3", MESSAGE));

            assertThat(result.isQuarantined()).isTrue();
            assertThat(result.getErrorCode()).isEqualTo("API_ERROR");
            assertThat(hdfs.readFile(BASE + "/errors/" + eventIdGenerator.generateEventId("MSG-3") + ".xml"))
                    .isEqualTo(MESSAGE);
            assertThat(audit.hasEventOfType(AuditEventType.API_CALL_FAILED)).isTrue();
        }

        @Test
        @DisplayName("returns FAILURE and leaves no file when the HDFS write fails")
        void hdfsFailureLeavesNothing() throws Exception {
            server.enqueue(new MockResponse().setBody(RESPONSE));
            hdfs.setShouldFailOnCreate(true);

            ProcessingResult result = orchestrator.process(message("MSG-4", MESSAGE));

            assertThat(result.isFailed()).isTrue();
            assertThat(result.getErrorCode()).isEqualTo("HDFS_ERROR");
            assertThat(hdfs.listFiles(BASE + "/2026-09-13/04")).isEmpty();
        }
    }
}
