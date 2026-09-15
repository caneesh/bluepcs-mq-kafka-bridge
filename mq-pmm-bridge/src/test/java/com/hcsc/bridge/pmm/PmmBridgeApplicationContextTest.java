package com.hcsc.bridge.pmm;

import com.hcsc.bridge.diagnostics.ReadinessCheckService;
import com.hcsc.bridge.pmm.api.PmmApiClient;
import com.hcsc.bridge.diagnostics.ReadinessCheck;
import com.hcsc.bridge.pmm.local.LocalPmmApiClient;
import com.hcsc.bridge.pmm.orchestrator.PmmOrchestrator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jms.config.JmsListenerEndpointRegistry;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@ActiveProfiles("local")
@TestPropertySource(properties = {
        "bridge.mq.listener-enabled=false",
        "bridge.validate-only=false"
})
@DisplayName("PmmBridgeApplication context (local profile)")
class PmmBridgeApplicationContextTest {

    @Autowired private PmmOrchestrator orchestrator;
    @Autowired private PmmApiClient apiClient;
    @Autowired private ReadinessCheckService readinessCheckService;
    @Autowired private java.util.List<ReadinessCheck> readinessChecks;
    @Autowired(required = false) private JmsListenerEndpointRegistry registry;

    @Test
    @DisplayName("wires the PMM pipeline with the local fakes from bridge-core")
    void wiresLocalPipeline() {
        assertThat(orchestrator).isNotNull();
        assertThat(apiClient).isInstanceOf(LocalPmmApiClient.class);
        // The PMM bridge does not replace the shared readiness service: it publishes its own
        // checks and the service composes whatever is on the classpath.
        assertThat(readinessChecks).extracting(ReadinessCheck::name)
                .contains("MQ_CONNECTION", "HDFS_CONNECTION", "OAUTH_TOKEN",
                        "PMM_API_REACHABLE", "PMM_TEMPLATE");
        if (registry != null) {
            registry.getListenerContainers().forEach(c -> assertThat(c.isRunning()).isFalse());
        }
    }
}
