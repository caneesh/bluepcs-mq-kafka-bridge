package com.hcsc.bridge.health;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.jms.config.JmsListenerEndpointRegistry;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jms.listener.MessageListenerContainer;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@DisplayName("MqConsumerHealthIndicator")
class MqConsumerHealthIndicatorTest {

    @Mock
    private JmsListenerEndpointRegistry registry;

    /**
     * isRunning() and getActiveConsumerCount() are final on the Spring container, so a
     * Mockito subclass mock cannot stub them: use a real container and set the private
     * lifecycle/registration counters it derives those answers from.
     */
    private DefaultMessageListenerContainer container(boolean running, boolean registered, int active) {
        DefaultMessageListenerContainer c = new DefaultMessageListenerContainer();
        ReflectionTestUtils.setField(c, "running", running);
        ReflectionTestUtils.setField(c, "registeredWithDestination", registered ? 1 : 0);
        ReflectionTestUtils.setField(c, "activeInvokerCount", active);
        return c;
    }

    @Test
    @DisplayName("is UP only while the running container holds a consumer on the queue")
    void upWhenRegistered() {
        DefaultMessageListenerContainer registered = container(true, true, 1);
        when(registry.getListenerContainers()).thenReturn(List.<MessageListenerContainer>of(registered));

        Health health = new MqConsumerHealthIndicator(registry, true).health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("registeredContainers", 1L).containsEntry("activeConsumers", 1);
    }

    @Test
    @DisplayName("is DOWN when the container is running but no consumer is registered (auth or connection failure)")
    void downWhenRunningButNotRegistered() {
        DefaultMessageListenerContainer unregistered = container(true, false, 1);
        when(registry.getListenerContainers()).thenReturn(List.<MessageListenerContainer>of(unregistered));

        Health health = new MqConsumerHealthIndicator(registry, true).health();

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry("runningContainers", 1L).containsEntry("registeredContainers", 0L);
        assertThat(health.getDetails().get("error").toString()).contains("no consumer on the queue");
    }

    @Test
    @DisplayName("is UP with a warning when the listener is deliberately disabled")
    void upWithWarningWhenDisabled() {
        DefaultMessageListenerContainer stopped = container(false, false, 0);
        when(registry.getListenerContainers()).thenReturn(List.<MessageListenerContainer>of(stopped));

        Health health = new MqConsumerHealthIndicator(registry, false).health();

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry("listenerEnabled", false).containsKey("warning");
    }

    @Test
    @DisplayName("is DOWN when no container is registered at all")
    void downWhenNoContainers() {
        when(registry.getListenerContainers()).thenReturn(List.of());

        assertThat(new MqConsumerHealthIndicator(registry, true).health().getStatus()).isEqualTo(Status.DOWN);
    }
}
