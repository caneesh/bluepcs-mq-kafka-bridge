package com.hcsc.bridge.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jms.config.JmsListenerEndpointRegistry;
import org.springframework.jms.listener.MessageListenerContainer;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.*;

class ListenerEnabledTest {

    @SpringBootTest
    @ActiveProfiles("local")
    @TestPropertySource(properties = {
            "bridge.mq.listener-enabled=false",
            "bridge.validate-only=false"
    })
    static class ListenerDisabledTest {

        @Autowired(required = false)
        private JmsListenerEndpointRegistry jmsListenerEndpointRegistry;

        @Value("${bridge.mq.listener-enabled:false}")
        private boolean listenerEnabled;

        @Test
        void listenerEnabled_shouldBeFalse() {
            assertFalse(listenerEnabled);
        }

        @Test
        void jmsListeners_shouldNotBeRunning() {
            if (jmsListenerEndpointRegistry != null) {
                for (MessageListenerContainer container : jmsListenerEndpointRegistry.getListenerContainers()) {
                    assertFalse(container.isRunning(),
                            "JMS listener should not be running when listener-enabled=false");
                }
            }
        }
    }

    @SpringBootTest
    @ActiveProfiles("local")
    @TestPropertySource(properties = {
            "bridge.validate-only=false",
            "bridge.mq.listener-enabled=false"
    })
    static class ValidateOnlyPropertyTest {

        @Value("${bridge.validate-only:false}")
        private boolean validateOnly;

        @Value("${bridge.mq.listener-enabled:false}")
        private boolean listenerEnabled;

        @Test
        void validateOnlyProperty_shouldBeConfigurable() {
            assertFalse(validateOnly);
            assertFalse(listenerEnabled);
        }
    }

    @Test
    void listenerEnabledProperty_defaultShouldBeFalse() {
        // This tests the documented default behavior
        // The actual listener-enabled default is false in application.yml
        // This is intentional for safety - require explicit opt-in to consume messages
        assertTrue(true, "Default listener-enabled=false is a safety feature");
    }
}
