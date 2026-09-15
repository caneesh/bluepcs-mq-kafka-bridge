package com.hcsc.bridge.pmm.config;

import com.hcsc.bridge.diagnostics.ReadinessCheckService;
import com.hcsc.bridge.diagnostics.ReadinessCheckService.CheckResult;
import com.hcsc.bridge.diagnostics.ReadinessCheckService.ReadinessReport;
import com.hcsc.bridge.pmm.template.PmmRequestTemplate;
import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.test.util.ReflectionTestUtils;

import java.net.ServerSocket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("PmmReadinessCheckService")
class PmmReadinessCheckServiceTest {

    @SuppressWarnings("unchecked")
    private PmmReadinessCheckService service(String apiUrl, PmmRequestTemplate template) {
        ObjectProvider<Configuration> hadoop = mock(ObjectProvider.class);
        ObjectProvider<PmmRequestTemplate> templates = mock(ObjectProvider.class);
        when(templates.getIfAvailable()).thenReturn(template);
        PmmReadinessCheckService service = new PmmReadinessCheckService(hadoop, templates, apiUrl);
        // No infrastructure configured: the inherited checks all SKIP
        ReflectionTestUtils.setField(service, "mqHost", "");
        ReflectionTestUtils.setField(service, "kafkaBootstrapServers", "");
        ReflectionTestUtils.setField(service, "hdfsNamenode", "");
        ReflectionTestUtils.setField(service, "oauthTokenUrl", "");
        return service;
    }

    @Test
    @DisplayName("adds PMM_API_REACHABLE and PMM_TEMPLATE to the shared checks")
    void addsPmmChecks() throws Exception {
        try (ServerSocket listening = new ServerSocket(0)) {
            PmmRequestTemplate template = new PmmRequestTemplate("<r>${value1}${value2}</r>", "inline", true);
            ReadinessReport report = service("http://localhost:" + listening.getLocalPort() + "/pmm", template)
                    .runAllChecks();

            assertThat(report.getResults()).extracting(CheckResult::getName)
                    .contains("MQ_CONNECTION", "KAFKA_CONNECTION", "HDFS_CONNECTION", "OAUTH_TOKEN",
                            "PMM_API_REACHABLE", "PMM_TEMPLATE");
            assertThat(report.getResults()).filteredOn(r -> r.getName().startsWith("PMM_"))
                    .allSatisfy(r -> assertThat(r.isPassed()).isTrue());
            assertThat(report.isPassed()).isTrue();
        }
    }

    @Test
    @DisplayName("fails when the API host is unreachable or the template is missing")
    void failsWhenUnreachable() {
        ReadinessReport report = service("http://localhost:1/pmm", null).runAllChecks();

        assertThat(report.getResults()).filteredOn(r -> r.getName().startsWith("PMM_"))
                .allSatisfy(r -> assertThat(r.isFailed()).isTrue());
        assertThat(report.isPassed()).isFalse();
    }
}
