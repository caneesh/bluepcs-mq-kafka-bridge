package com.hcsc.bridge.config;

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

    @org.junit.jupiter.api.Nested
    class ListenerGate {

        private final StartupConfigValidator validator = new StartupConfigValidator();
        private final java.util.List<String> errors = new java.util.ArrayList<>();
        private final java.util.List<String> warnings = new java.util.ArrayList<>();

        private void configure(boolean listenerEnabled, boolean requireListenerEnabled,
                               boolean validateOnly, String componentTest, boolean monitorEnabled) {
            org.springframework.test.util.ReflectionTestUtils.setField(validator, "mqListenerEnabled", listenerEnabled);
            org.springframework.test.util.ReflectionTestUtils.setField(validator, "requireListenerEnabled", requireListenerEnabled);
            org.springframework.test.util.ReflectionTestUtils.setField(validator, "validateOnly", validateOnly);
            org.springframework.test.util.ReflectionTestUtils.setField(validator, "componentTestMode", componentTest);
            org.springframework.test.util.ReflectionTestUtils.setField(validator, "monitorEnabled", monitorEnabled);
        }

        @Test
        void safeStartDefaultRemainsListenerDisabledWithoutError() {
            // Default posture (require=false): listener off is a warning, never an error
            configure(false, false, false, "", false);
            validator.validateListenerGate(errors, warnings);
            assertTrue(errors.isEmpty());
            assertEquals(1, warnings.size());
        }

        @Test
        void gateFailsWhenRequiredAndListenerDisabled() {
            configure(false, true, false, "", false);
            validator.validateListenerGate(errors, warnings);
            assertEquals(1, errors.size());
            assertTrue(errors.get(0).contains("require-listener-enabled"));
        }

        @Test
        void gatePassesWhenRequiredAndListenerEnabled() {
            configure(true, true, false, "", false);
            validator.validateListenerGate(errors, warnings);
            assertTrue(errors.isEmpty());
            assertTrue(warnings.isEmpty());
        }

        @Test
        void validateOnlyModeIsExemptFromGate() {
            configure(false, true, true, "", false);
            validator.validateListenerGate(errors, warnings);
            assertTrue(errors.isEmpty());
            assertTrue(warnings.isEmpty());
        }

        @Test
        void componentTestModeIsExemptFromGate() {
            configure(false, true, false, "hdfs", false);
            validator.validateListenerGate(errors, warnings);
            assertTrue(errors.isEmpty());
        }

        @Test
        void monitorModeIsExemptFromGate() {
            configure(false, true, false, "", true);
            validator.validateListenerGate(errors, warnings);
            assertTrue(errors.isEmpty());
        }
    }
}
