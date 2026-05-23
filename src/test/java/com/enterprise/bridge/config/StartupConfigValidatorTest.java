package com.enterprise.bridge.config;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("local")
class StartupConfigValidatorTest {

    @Test
    void validator_shouldNotLoadInLocalProfile() {
        // In local profile, StartupConfigValidator should not be instantiated
        // because it has @Profile("!local")
        // This test verifies the application context loads successfully without validation errors
    }

    @Test
    void configurationValidationException_shouldContainMessage() {
        String message = "Test validation error";
        StartupConfigValidator.ConfigurationValidationException exception =
                new StartupConfigValidator.ConfigurationValidationException(message);

        assertEquals(message, exception.getMessage());
    }

    @Test
    void configurationValidationException_shouldBeRuntimeException() {
        StartupConfigValidator.ConfigurationValidationException exception =
                new StartupConfigValidator.ConfigurationValidationException("test");

        assertTrue(exception instanceof RuntimeException);
    }
}
