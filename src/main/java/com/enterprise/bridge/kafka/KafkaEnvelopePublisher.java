package com.enterprise.bridge.kafka;

import com.enterprise.bridge.model.KafkaEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
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
    private final ObjectMapper objectMapper;
    private final String topic;
    private final long timeoutSeconds;

    public KafkaEnvelopePublisher(
            KafkaTemplate<String, String> kafkaTemplate,
            @Value("${bridge.kafka.topic:bridge-events}") String topic,
            @Value("${bridge.kafka.timeout-seconds:30}") long timeoutSeconds) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
        this.timeoutSeconds = timeoutSeconds;
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
    }

    public String publish(KafkaEnvelope envelope) {
        String messageId = envelope.getMessageId();
        String key = envelope.getKafkaKey();

        logger.debug("Publishing envelope to Kafka: messageId={}, key={}, topic={}",
                messageId, key, topic);

        try {
            String payload = objectMapper.writeValueAsString(envelope);

            ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, key, payload);
            SendResult<String, String> result = future.get(timeoutSeconds, TimeUnit.SECONDS);

            String offset = String.valueOf(result.getRecordMetadata().offset());
            int partition = result.getRecordMetadata().partition();

            logger.info("Published envelope to Kafka: messageId={}, topic={}, partition={}, offset={}",
                    messageId, topic, partition, offset);

            return offset;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException("Interrupted while publishing to Kafka", messageId, topic, e);
        } catch (ExecutionException e) {
            throw new KafkaPublishException("Failed to publish to Kafka", messageId, topic, e.getCause());
        } catch (TimeoutException e) {
            throw new KafkaPublishException("Timeout publishing to Kafka", messageId, topic, e);
        } catch (Exception e) {
            throw new KafkaPublishException("Unexpected error publishing to Kafka", messageId, topic, e);
        }
    }
}
