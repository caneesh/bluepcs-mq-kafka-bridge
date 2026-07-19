package com.hcsc.bridge.health;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("KafkaHealthIndicator")
class KafkaHealthIndicatorTest {

    @Mock
    private AdminClient adminClient;

    @AfterEach
    void clearInterruptFlag() {
        // Never leak an interrupt flag set by a test into the rest of the suite
        Thread.interrupted();
    }

    @Test
    @DisplayName("should be DOWN and restore the interrupt flag when interrupted")
    @SuppressWarnings("unchecked")
    void shouldRestoreInterruptFlagWhenInterrupted() throws Exception {
        DescribeClusterResult result = mock(DescribeClusterResult.class);
        KafkaFuture<String> clusterIdFuture = mock(KafkaFuture.class);
        when(clusterIdFuture.get(anyLong(), any())).thenThrow(new InterruptedException("shutdown"));
        when(result.clusterId()).thenReturn(clusterIdFuture);
        lenient().when(adminClient.describeCluster()).thenReturn(result);

        KafkaHealthIndicator indicator = new KafkaHealthIndicator(adminClient);

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("error", "Health check interrupted");
        // The flag must survive so the actuator/watchdog thread sees the shutdown
        assertThat(Thread.currentThread().isInterrupted()).isTrue();
    }

    @Test
    @DisplayName("should be DOWN with the error message on other failures")
    void shouldBeDownOnFailure() {
        when(adminClient.describeCluster()).thenThrow(new RuntimeException("broker unreachable"));

        KafkaHealthIndicator indicator = new KafkaHealthIndicator(adminClient);

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("error", "broker unreachable");
    }
}
