package com.hcsc.bridge.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Component
public class KafkaEnvelopePublisher {

    private static final Logger logger = LoggerFactory.getLogger(KafkaEnvelopePublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final String topic;
    private final long timeoutSeconds;

    public KafkaEnvelopePublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${bridge.kafka.topic:bridge-events}") String topic,
            // Default covers max.block.ms (60s) + delivery.timeout.ms (120s) with margin:
            // a wait shorter than the producer's own budget times out while the record is
            // still in flight, reporting failure for a message that is usually delivered
            @Value("${bridge.kafka.timeout-seconds:190}") long timeoutSeconds) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.timeoutSeconds = timeoutSeconds;
    }

    /**
     * Publishes the claim-check notification JSON to Kafka synchronously. The full wrapper
     * document lives in HDFS; the value here is the small notification built by
     * {@link KafkaNotificationFactory}.
     *
     * @param key   the Kafka message key (the eventId, supplied by the orchestrator)
     * @param value the claim-check notification JSON string (message value)
     * @return the offset the record was written to, as a string
     */
    public String publish(String key, String value) {
        logger.debug("Publishing to Kafka: key={}, topic={}", key, topic);

        try {
            ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, value);
            SendResult<String, String> result = future.get(timeoutSeconds, TimeUnit.SECONDS);

            String offset = String.valueOf(result.getRecordMetadata().offset());
            int partition = result.getRecordMetadata().partition();

            logger.info("Published to Kafka: key={}, topic={}, partition={}, offset={}",
                    key, topic, partition, offset);

            return offset;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException("Interrupted while publishing to Kafka", key, topic, e);
        } catch (ExecutionException e) {
            // getCause() is the real producer failure, but fall back to the
            // ExecutionException itself rather than throwing with no cause at all
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new KafkaPublishException("Failed to publish to Kafka", key, topic, cause);
        } catch (TimeoutException e) {
            throw new KafkaPublishException("Timeout publishing to Kafka", key, topic, e);
        } catch (Exception e) {
            throw new KafkaPublishException("Unexpected error publishing to Kafka", key, topic, e);
        }
    }
}
