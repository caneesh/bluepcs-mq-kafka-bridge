package com.hcsc.bridge.health;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.context.annotation.Profile;
import org.springframework.jms.config.JmsListenerEndpointRegistry;
import org.springframework.jms.listener.DefaultMessageListenerContainer;
import org.springframework.jms.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

import java.util.Collection;

/**
 * Reports whether the bridge actually holds a consumer on its queue.
 *
 * <p>{@link MqListenerHealthIndicator} answers a lifecycle question ({@code isRunning()}),
 * and a {@link DefaultMessageListenerContainer} keeps running — retrying in the
 * background — through a failed connection or a queue-manager authorization failure. That
 * is the right signal for the process supervisors (a broker outage must not restart-loop
 * a healthy JVM), which is why only {@code mqListener} is in the liveness group. It is the
 * wrong signal for "is anything being consumed": this indicator fills that gap with
 * {@link DefaultMessageListenerContainer#isRegisteredWithDestination()}, which is true only
 * while a JMS consumer object exists on the queue and is cleared the moment the container
 * loses it. {@code MonitorRunner} turns it into the Control-M "not consuming" exit code.
 */
@Component("mqConsumerHealthIndicator")
@Profile("!local")
public class MqConsumerHealthIndicator implements HealthIndicator {

    private static final Logger logger = LoggerFactory.getLogger(MqConsumerHealthIndicator.class);

    private final JmsListenerEndpointRegistry listenerRegistry;
    private final boolean listenerEnabled;

    public MqConsumerHealthIndicator(
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
            if (!listenerEnabled) {
                return Health.up()
                        .withDetail("listenerEnabled", false)
                        .withDetail("registeredContainers", 0)
                        .withDetail("totalContainers", containers.size())
                        .withDetail("warning", "Listener disabled (bridge.mq.listener-enabled=false): "
                                + "no consumer is registered with the queue")
                        .build();
            }

            long running = 0;
            long registered = 0;
            int activeConsumers = 0;
            for (MessageListenerContainer container : containers) {
                if (container.isRunning()) {
                    running++;
                }
                if (container instanceof DefaultMessageListenerContainer) {
                    DefaultMessageListenerContainer dmlc = (DefaultMessageListenerContainer) container;
                    if (dmlc.isRegisteredWithDestination()) {
                        registered++;
                    }
                    activeConsumers += dmlc.getActiveConsumerCount();
                } else if (container.isRunning()) {
                    // Other container types expose no registration state; running is all we know
                    registered++;
                }
            }

            Health.Builder builder = registered == containers.size() ? Health.up() : Health.down();
            builder.withDetail("listenerEnabled", true)
                    .withDetail("runningContainers", running)
                    .withDetail("registeredContainers", registered)
                    .withDetail("totalContainers", containers.size())
                    .withDetail("activeConsumers", activeConsumers);
            if (registered != containers.size()) {
                logger.warn("MQ consumer health check: {}/{} containers hold a consumer on the queue "
                        + "({} running)", registered, containers.size(), running);
                builder.withDetail("error", "Listener container is running but holds no consumer on the queue "
                        + "(MQ connection or authorization failure - see 'JMS listener error' log lines)");
            }
            return builder.build();
        } catch (Exception e) {
            logger.warn("MQ consumer health check failed: {}", e.getMessage());
            return Health.down().withDetail("error", e.getMessage()).build();
        }
    }
}
