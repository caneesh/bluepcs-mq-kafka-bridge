package com.hcsc.bridge.health;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.jms.config.JmsListenerEndpointRegistry;
import org.springframework.jms.listener.MessageListenerContainer;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("MqListenerHealthIndicator")
class MqListenerHealthIndicatorTest {

    @Mock
    private JmsListenerEndpointRegistry registry;

    private MessageListenerContainer container(boolean running) {
        MessageListenerContainer container = mock(MessageListenerContainer.class);
        when(container.isRunning()).thenReturn(running);
        return container;
    }

    @Test
    @DisplayName("should be UP when the listener is enabled and running")
    void shouldBeUpWhenRunning() {
        MessageListenerContainer running = container(true);
        when(registry.getListenerContainers()).thenReturn(List.of(running));
        MqListenerHealthIndicator indicator = new MqListenerHealthIndicator(registry, true);

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("listenerEnabled", true);
        assertThat(health.getDetails()).containsEntry("runningContainers", 1L);
    }

    @Test
    @DisplayName("should be DOWN when the listener is enabled but not running")
    void shouldBeDownWhenEnabledButNotRunning() {
        MessageListenerContainer stopped = container(false);
        when(registry.getListenerContainers()).thenReturn(List.of(stopped));
        MqListenerHealthIndicator indicator = new MqListenerHealthIndicator(registry, true);

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("error", "JMS listener container is not running");
    }

    @Test
    @DisplayName("should stay UP with a warning when the listener is intentionally disabled")
    void shouldStayUpWhenDisabledByConfiguration() {
        MessageListenerContainer stopped = container(false);
        when(registry.getListenerContainers()).thenReturn(List.of(stopped));
        MqListenerHealthIndicator indicator = new MqListenerHealthIndicator(registry, false);

        Health health = indicator.health();

        // Safe-start mode: health must not fail before the listener is enabled,
        // but the not-consuming state must be loudly visible in the details
        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("listenerEnabled", false);
        assertThat(health.getDetails().get("warning").toString()).contains("NOT consuming");
    }

    @Test
    @DisplayName("should be DOWN when no listener containers are registered")
    void shouldBeDownWhenNoContainers() {
        when(registry.getListenerContainers()).thenReturn(List.of());
        MqListenerHealthIndicator indicator = new MqListenerHealthIndicator(registry, true);

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("error", "No JMS listener containers registered");
    }

    @Test
    @DisplayName("should be DOWN when the registry throws")
    void shouldBeDownWhenRegistryThrows() {
        when(registry.getListenerContainers()).thenThrow(new IllegalStateException("registry not ready"));
        MqListenerHealthIndicator indicator = new MqListenerHealthIndicator(registry, true);

        Health health = indicator.health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("error", "registry not ready");
    }
}
