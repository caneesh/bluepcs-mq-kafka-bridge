package com.hcsc.bridge.health;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.annotation.Profile;
import org.springframework.jms.config.JmsListenerEndpointRegistry;
import org.springframework.jms.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.Collection;

/**
 * Reports whether the MQ listener container is actually consuming. Without this,
 * /actuator/health stays UP while the bridge silently consumes nothing (listener
 * crashed, or never enabled).
 *
 * Status semantics:
 * - UP, listener running: normal 24/7 operation.
 * - UP with {@code listenerEnabled=false} detail: safe-start mode — intentional, so
 *   health must not fail (the smoke test and the documented deploy flow verify health
 *   BEFORE enabling the listener). Monitoring should still alert on this detail in
 *   production, and MQ-side queue-depth alerts remain the definitive consumption signal.
 * - DOWN: the listener is enabled but a container is not running — a real fault.
 */
@Component
@Profile("!local")
public class MqListenerHealthIndicator implements HealthIndicator {

    private static final Logger logger = LoggerFactory.getLogger(MqListenerHealthIndicator.class);

    private final JmsListenerEndpointRegistry listenerRegistry;
    private final boolean listenerEnabled;

    public MqListenerHealthIndicator(
            JmsListenerEndpointRegistry listenerRegistry,
            @Value("${bridge.mq.listener-enabled:false}") boolean listenerEnabled) {
        this.listenerRegistry = listenerRegistry;
        this.listenerEnabled = listenerEnabled;
    }

    @Override
    public Health health() {
        try {
            Collection<MessageListenerContainer> containers = listenerRegistry.getListenerContainers();

            if (containers.isEmpty()) {
                return Health.down()
                        .withDetail("listenerEnabled", listenerEnabled)
                        .withDetail("error", "No JMS listener containers registered")
                        .build();
            }

            long running = containers.stream().filter(MessageListenerContainer::isRunning).count();
            boolean allRunning = running == containers.size();

            if (allRunning) {
                return Health.up()
                        .withDetail("listenerEnabled", listenerEnabled)
                        .withDetail("runningContainers", running)
                        .withDetail("totalContainers", containers.size())
                        .build();
            }

            if (!listenerEnabled) {
                return Health.up()
                        .withDetail("listenerEnabled", false)
                        .withDetail("runningContainers", running)
                        .withDetail("totalContainers", containers.size())
                        .withDetail("warning", "Listener disabled (bridge.mq.listener-enabled=false): "
                                + "the bridge is NOT consuming messages")
                        .build();
            }

            logger.warn("MQ listener health check: {}/{} containers running with listener enabled",
                    running, containers.size());
            return Health.down()
                    .withDetail("listenerEnabled", true)
                    .withDetail("runningContainers", running)
                    .withDetail("totalContainers", containers.size())
                    .withDetail("error", "JMS listener container is not running")
                    .build();

        } catch (Exception e) {
            logger.warn("MQ listener health check failed: {}", e.getMessage());
            return Health.down()
                    .withDetail("error", e.getMessage())
                    .build();
        }
    }
}
