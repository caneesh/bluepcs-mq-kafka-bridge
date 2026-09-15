package com.hcsc.bridge.pmm;

import com.hcsc.bridge.diagnostics.ReadinessCheckService;
import com.hcsc.bridge.pmm.api.PmmApiClient;
import com.hcsc.bridge.pmm.config.PmmReadinessCheckService;
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
    @Autowired(required = false) private JmsListenerEndpointRegistry registry;

    @Test
    @DisplayName("wires the PMM pipeline with the local fakes from bridge-core")
    void wiresLocalPipeline() {
        assertThat(orchestrator).isNotNull();
        assertThat(apiClient).isInstanceOf(LocalPmmApiClient.class);
        assertThat(readinessCheckService).isInstanceOf(PmmReadinessCheckService.class);
        if (registry != null) {
            registry.getListenerContainers().forEach(c -> assertThat(c.isRunning()).isFalse());
        }
    }
}
