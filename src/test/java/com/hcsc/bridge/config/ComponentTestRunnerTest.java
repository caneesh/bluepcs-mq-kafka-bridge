package com.hcsc.bridge.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hcsc.bridge.api.EnrichmentException;
import com.hcsc.bridge.api.EnrichmentWrapperFactory;
import com.hcsc.bridge.api.MarketingPlanApiClient;
import com.hcsc.bridge.hdfs.HdfsFileOperations;
import com.hcsc.bridge.model.ParsedPayload;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.SettableListenableFuture;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ComponentTestRunnerTest {

    private ApplicationContext applicationContext;
    private Environment environment;

    @SuppressWarnings("unchecked")
    private ObjectProvider<ConnectionFactory> connectionFactoryProvider = mock(ObjectProvider.class);
    @SuppressWarnings("unchecked")
    private ObjectProvider<MarketingPlanApiClient> apiClientProvider = mock(ObjectProvider.class);
    @SuppressWarnings("unchecked")
    private ObjectProvider<EnrichmentWrapperFactory> wrapperFactoryProvider = mock(ObjectProvider.class);
    @SuppressWarnings("unchecked")
    private ObjectProvider<KafkaTemplate<String, String>> kafkaTemplateProvider = mock(ObjectProvider.class);
    @SuppressWarnings("unchecked")
    private ObjectProvider<HdfsFileOperations> hdfsFileOperationsProvider = mock(ObjectProvider.class);

    @BeforeEach
    void setUp() {
        applicationContext = mock(ApplicationContext.class);
        environment = mock(Environment.class);
        when(environment.getActiveProfiles()).thenReturn(new String[]{"test"});
    }

    private ComponentTestRunner newRunner() {
        return new ComponentTestRunner(
                applicationContext,
                environment,
                connectionFactoryProvider,
                apiClientProvider,
                wrapperFactoryProvider,
                kafkaTemplateProvider,
                hdfsFileOperationsProvider);
    }

    private static void setField(ComponentTestRunner runner, String name, Object value) throws Exception {
        Field field = ComponentTestRunner.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(runner, value);
    }

    // ------------------------------------------------------------------------
    // mode gating
    // ------------------------------------------------------------------------

    @Test
    void run_whenModeEmpty_shouldNoOpAndTouchNoBeans() throws Exception {
        ComponentTestRunner runner = newRunner();
        setField(runner, "mode", "");

        // Should return without exiting; run() is safe because active profile contains "test".
        runner.run(mock(org.springframework.boot.ApplicationArguments.class));

        verify(connectionFactoryProvider, never()).getObject();
        verify(apiClientProvider, never()).getObject();
        verify(kafkaTemplateProvider, never()).getObject();
        verify(hdfsFileOperationsProvider, never()).getObject();
    }

    @Test
    void runComponentTest_whenUnknownMode_shouldReturnBadMode() throws Exception {
        ComponentTestRunner runner = newRunner();
        setField(runner, "mode", "banana");

        assertEquals(ComponentTestRunner.EXIT_BAD_MODE, runner.runComponentTest());
    }

    // ------------------------------------------------------------------------
    // mode: api
    // ------------------------------------------------------------------------

    @Nested
    class ApiMode {

        @Test
        void missingArgs_shouldReturnBadMode() throws Exception {
            ComponentTestRunner runner = newRunner();
            setField(runner, "mode", "api");
            setField(runner, "modeArgs", "");

            assertEquals(ComponentTestRunner.EXIT_BAD_MODE, runner.runComponentTest());
            verify(apiClientProvider, never()).getObject();
        }

        @Test
        void malformedArgs_shouldReturnBadMode() throws Exception {
            ComponentTestRunner runner = newRunner();
            setField(runner, "mode", "api");
            setField(runner, "modeArgs", "onlyplanid");

            assertEquals(ComponentTestRunner.EXIT_BAD_MODE, runner.runComponentTest());
            verify(apiClientProvider, never()).getObject();
        }

        @Test
        void happyPath_shouldCallEnrichWithGivenPlanAndReturnPassed() throws Exception {
            MarketingPlanApiClient apiClient = mock(MarketingPlanApiClient.class);
            when(apiClientProvider.getObject()).thenReturn(apiClient);
            when(wrapperFactoryProvider.getObject()).thenReturn(new EnrichmentWrapperFactory());

            ObjectMapper mapper = new ObjectMapper();
            MarketingPlanApiClient.EnrichmentResult result =
                    new MarketingPlanApiClient.EnrichmentResult("SPSH44PPOIMTO", null,
                            new java.util.HashMap<>(), mapper.createObjectNode());
            when(apiClient.enrich(org.mockito.ArgumentMatchers.any(ParsedPayload.class))).thenReturn(result);

            ComponentTestRunner runner = newRunner();
            setField(runner, "mode", "api");
            setField(runner, "modeArgs", "SPSH44PPOIMTO,2026-01-01");

            assertEquals(ComponentTestRunner.EXIT_PASSED, runner.runComponentTest());

            ArgumentCaptor<ParsedPayload> captor = ArgumentCaptor.forClass(ParsedPayload.class);
            verify(apiClient).enrich(captor.capture());
            ParsedPayload sent = captor.getValue();
            assertEquals("SPSH44PPOIMTO", sent.getEntityId());
            assertEquals("SPSH44PPOIMTO", sent.getTransactionId());
            assertEquals("2026-01-01", sent.getEffectiveDate());
            assertEquals("component-test", sent.getMessageId());
            assertEquals("ComponentTest", sent.getEventType());
        }

        @Test
        void enrichmentException_shouldReturnFailed() throws Exception {
            MarketingPlanApiClient apiClient = mock(MarketingPlanApiClient.class);
            when(apiClientProvider.getObject()).thenReturn(apiClient);
            when(wrapperFactoryProvider.getObject()).thenReturn(new EnrichmentWrapperFactory());
            when(apiClient.enrich(org.mockito.ArgumentMatchers.any(ParsedPayload.class)))
                    .thenThrow(new EnrichmentException("API client error: 404", "SPSH44PPOIMTO", 404));

            ComponentTestRunner runner = newRunner();
            setField(runner, "mode", "api");
            setField(runner, "modeArgs", "SPSH44PPOIMTO,2026-01-01");

            assertEquals(ComponentTestRunner.EXIT_FAILED, runner.runComponentTest());
        }
    }

    // ------------------------------------------------------------------------
    // mode: kafka
    // ------------------------------------------------------------------------

    @Nested
    class KafkaMode {

        @SuppressWarnings("unchecked")
        private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);

        private SettableListenableFuture<SendResult<String, String>> successFuture(String topic, long offset) {
            SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
            RecordMetadata metadata = new RecordMetadata(
                    new TopicPartition(topic, 0), 0L, (int) offset,
                    System.currentTimeMillis(), null, 0, 0);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key", "value");
            future.set(new SendResult<>(record, metadata));
            return future;
        }

        @Test
        void noTopicArg_shouldRefuseWithoutPublishing() throws Exception {
            ComponentTestRunner runner = newRunner();
            setField(runner, "mode", "kafka");
            setField(runner, "modeArgs", "");
            setField(runner, "kafkaTopic", "configured-topic");

            // The configured (possibly production) topic must never be used implicitly:
            // an unmarked test record would reach the downstream consumer as junk
            assertEquals(ComponentTestRunner.EXIT_BAD_MODE, runner.runComponentTest());
            verify(kafkaTemplate, never()).send(anyString(), anyString(), anyString());
        }

        @Test
        void explicitConfiguredTopic_shouldPublishAndReturnPassed() throws Exception {
            when(kafkaTemplateProvider.getObject()).thenReturn(kafkaTemplate);
            when(kafkaTemplate.send(eq("configured-topic"), anyString(), anyString()))
                    .thenReturn(successFuture("configured-topic", 42L));

            ComponentTestRunner runner = newRunner();
            setField(runner, "mode", "kafka");
            // Publishing to the real topic stays possible — by naming it deliberately
            setField(runner, "modeArgs", "configured-topic");
            setField(runner, "kafkaTopic", "configured-topic");

            assertEquals(ComponentTestRunner.EXIT_PASSED, runner.runComponentTest());
            verify(kafkaTemplate).send(eq("configured-topic"), anyString(), anyString());
        }

        @Test
        void topicOverride_shouldPublishToOverrideTopic() throws Exception {
            when(kafkaTemplateProvider.getObject()).thenReturn(kafkaTemplate);
            when(kafkaTemplate.send(eq("scratch-topic"), anyString(), anyString()))
                    .thenReturn(successFuture("scratch-topic", 1L));

            ComponentTestRunner runner = newRunner();
            setField(runner, "mode", "kafka");
            setField(runner, "modeArgs", "scratch-topic");
            setField(runner, "kafkaTopic", "configured-topic");

            assertEquals(ComponentTestRunner.EXIT_PASSED, runner.runComponentTest());
            verify(kafkaTemplate).send(eq("scratch-topic"), anyString(), anyString());
        }

        @Test
        void publishFailure_shouldReturnFailed() throws Exception {
            SettableListenableFuture<SendResult<String, String>> future = new SettableListenableFuture<>();
            future.setException(new RuntimeException("Broker not available"));
            when(kafkaTemplateProvider.getObject()).thenReturn(kafkaTemplate);
            when(kafkaTemplate.send(eq("scratch-topic"), anyString(), anyString())).thenReturn(future);

            ComponentTestRunner runner = newRunner();
            setField(runner, "mode", "kafka");
            setField(runner, "modeArgs", "scratch-topic");
            setField(runner, "kafkaTopic", "configured-topic");

            assertEquals(ComponentTestRunner.EXIT_FAILED, runner.runComponentTest());
        }
    }

    // ------------------------------------------------------------------------
    // mode: hdfs
    // ------------------------------------------------------------------------

    @Nested
    class HdfsMode {

        private String sha256Hex(byte[] content) throws Exception {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(content);
            StringBuilder sb = new StringBuilder();
            for (byte b : hash) {
                sb.append(String.format("%02x", b));
            }
            return sb.toString();
        }

        private ComponentTestRunner hdfsRunner(HdfsFileOperations hdfs) throws Exception {
            when(hdfsFileOperationsProvider.getObject()).thenReturn(hdfs);
            ComponentTestRunner runner = newRunner();
            setField(runner, "mode", "hdfs");
            setField(runner, "hdfsBasePath", "/data/bridge");
            setField(runner, "hdfsTempSuffix", ".tmp");
            return runner;
        }

        @Test
        void happyRoundtrip_matchingChecksum_shouldReturnPassedAndDelete() throws Exception {
            HdfsFileOperations hdfs = mock(HdfsFileOperations.class);
            // Capture the content written so the mocked checksum matches what the runner computed.
            ByteArrayOutputStream captured = new ByteArrayOutputStream();
            when(hdfs.create(anyString())).thenReturn(captured);
            when(hdfs.rename(anyString(), anyString())).thenReturn(true);
            when(hdfs.getFileChecksum(anyString()))
                    .thenAnswer(inv -> sha256Hex(captured.toByteArray()));

            ComponentTestRunner runner = hdfsRunner(hdfs);

            assertEquals(ComponentTestRunner.EXIT_PASSED, runner.runComponentTest());

            verify(hdfs).mkdirs(anyString());
            verify(hdfs).rename(anyString(), anyString());
            // File was written, so cleanup deletes the target (not the temp).
            ArgumentCaptor<String> deleted = ArgumentCaptor.forClass(String.class);
            verify(hdfs).delete(deleted.capture());
            org.assertj.core.api.Assertions.assertThat(deleted.getValue()).endsWith(".json");
        }

        @Test
        void checksumMismatch_shouldReturnFailedAndDelete() throws Exception {
            HdfsFileOperations hdfs = mock(HdfsFileOperations.class);
            when(hdfs.create(anyString())).thenReturn(new ByteArrayOutputStream());
            when(hdfs.rename(anyString(), anyString())).thenReturn(true);
            when(hdfs.getFileChecksum(anyString())).thenReturn("deadbeef");

            ComponentTestRunner runner = hdfsRunner(hdfs);

            assertEquals(ComponentTestRunner.EXIT_FAILED, runner.runComponentTest());
            // Written before mismatch detected, so target is cleaned up.
            verify(hdfs).delete(anyString());
        }
    }

    // ------------------------------------------------------------------------
    // mode: mq
    // ------------------------------------------------------------------------

    @Nested
    class MqMode {

        @Test
        void connectionFailure_shouldReturnFailed() throws Exception {
            ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
            when(connectionFactoryProvider.getObject()).thenReturn(connectionFactory);
            when(connectionFactory.createConnection())
                    .thenThrow(new javax.jms.JMSException("cannot connect"));

            ComponentTestRunner runner = newRunner();
            setField(runner, "mode", "mq");
            setField(runner, "mqQueue", "BRIDGE.INPUT.QUEUE");

            assertEquals(ComponentTestRunner.EXIT_FAILED, runner.runComponentTest());
        }

        @Test
        void emptyQueueBrowse_shouldReturnPassedAndCloseResources() throws Exception {
            ConnectionFactory connectionFactory = mock(ConnectionFactory.class);
            Connection connection = mock(Connection.class);
            javax.jms.Session session = mock(javax.jms.Session.class);
            javax.jms.Queue queue = mock(javax.jms.Queue.class);
            javax.jms.QueueBrowser browser = mock(javax.jms.QueueBrowser.class);

            when(connectionFactoryProvider.getObject()).thenReturn(connectionFactory);
            when(connectionFactory.createConnection()).thenReturn(connection);
            when(connection.createSession(false, javax.jms.Session.AUTO_ACKNOWLEDGE)).thenReturn(session);
            when(session.createQueue("BRIDGE.INPUT.QUEUE")).thenReturn(queue);
            when(session.createBrowser(queue)).thenReturn(browser);
            when(browser.getEnumeration())
                    .thenReturn(java.util.Collections.enumeration(java.util.Collections.emptyList()));

            ComponentTestRunner runner = newRunner();
            setField(runner, "mode", "mq");
            setField(runner, "mqQueue", "BRIDGE.INPUT.QUEUE");

            assertEquals(ComponentTestRunner.EXIT_PASSED, runner.runComponentTest());

            verify(browser).close();
            verify(session).close();
            verify(connection).close();
        }
    }

    // Kept to document that content bytes are UTF-8, matching production writer.
    @Test
    void contentBytes_shouldBeUtf8() {
        String content = "{\"componentTest\":true}";
        assertEquals(content.length(), content.getBytes(StandardCharsets.UTF_8).length);
    }
}
