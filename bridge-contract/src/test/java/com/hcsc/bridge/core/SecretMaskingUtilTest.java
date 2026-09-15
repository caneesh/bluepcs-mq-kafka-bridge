package com.hcsc.bridge.core;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SecretMaskingUtilTest {

    @Test
    void mask_shouldReturnMaskedValue() {
        assertEquals("********", SecretMaskingUtil.mask("mySecretPassword123"));
    }

    @Test
    void mask_shouldReturnNullForNull() {
        assertNull(SecretMaskingUtil.mask(null));
    }

    @Test
    void mask_shouldReturnEmptyForEmpty() {
        assertEquals("", SecretMaskingUtil.mask(""));
    }

    @Test
    void isSecretField_shouldDetectPasswordField() {
        assertTrue(SecretMaskingUtil.isSecretField("password"));
        assertTrue(SecretMaskingUtil.isSecretField("PASSWORD"));
        assertTrue(SecretMaskingUtil.isSecretField("mqPassword"));
        assertTrue(SecretMaskingUtil.isSecretField("mq_password"));
        assertTrue(SecretMaskingUtil.isSecretField("MQ_PASSWORD"));
    }

    @Test
    void isSecretField_shouldDetectSecretField() {
        assertTrue(SecretMaskingUtil.isSecretField("secret"));
        assertTrue(SecretMaskingUtil.isSecretField("clientSecret"));
        assertTrue(SecretMaskingUtil.isSecretField("client_secret"));
        assertTrue(SecretMaskingUtil.isSecretField("OAUTH_CLIENT_SECRET"));
    }

    @Test
    void isSecretField_shouldDetectTokenField() {
        assertTrue(SecretMaskingUtil.isSecretField("token"));
        assertTrue(SecretMaskingUtil.isSecretField("accessToken"));
        assertTrue(SecretMaskingUtil.isSecretField("access_token"));
        assertTrue(SecretMaskingUtil.isSecretField("bearerToken"));
    }

    @Test
    void isSecretField_shouldDetectCredentialField() {
        assertTrue(SecretMaskingUtil.isSecretField("credential"));
        assertTrue(SecretMaskingUtil.isSecretField("credentials"));
        assertTrue(SecretMaskingUtil.isSecretField("userCredential"));
    }

    @Test
    void isSecretField_shouldDetectApiKeyField() {
        assertTrue(SecretMaskingUtil.isSecretField("apikey"));
        assertTrue(SecretMaskingUtil.isSecretField("api_key"));
        assertTrue(SecretMaskingUtil.isSecretField("APIKEY"));
        assertTrue(SecretMaskingUtil.isSecretField("API_KEY"));
    }

    @Test
    void isSecretField_shouldNotDetectNonSecretField() {
        assertFalse(SecretMaskingUtil.isSecretField("username"));
        assertFalse(SecretMaskingUtil.isSecretField("host"));
        assertFalse(SecretMaskingUtil.isSecretField("port"));
        assertFalse(SecretMaskingUtil.isSecretField("queueManager"));
        assertFalse(SecretMaskingUtil.isSecretField("bootstrapServers"));
    }

    @Test
    void isSecretField_shouldHandleNullAndEmpty() {
        assertFalse(SecretMaskingUtil.isSecretField(null));
        assertFalse(SecretMaskingUtil.isSecretField(""));
    }

    @Test
    void maskIfSecret_shouldMaskSecretFields() {
        assertEquals("********", SecretMaskingUtil.maskIfSecret("password", "secret123"));
        assertEquals("********", SecretMaskingUtil.maskIfSecret("mqPassword", "secret123"));
        assertEquals("********", SecretMaskingUtil.maskIfSecret("OAUTH_CLIENT_SECRET", "secret123"));
    }

    @Test
    void maskIfSecret_shouldNotMaskNonSecretFields() {
        assertEquals("myuser", SecretMaskingUtil.maskIfSecret("username", "myuser"));
        assertEquals("localhost", SecretMaskingUtil.maskIfSecret("host", "localhost"));
        assertEquals("1414", SecretMaskingUtil.maskIfSecret("port", "1414"));
    }

    @Test
    void maskSecrets_shouldMaskPasswordInText() {
        String input = "Connecting with password=secret123 to host";
        String result = SecretMaskingUtil.maskSecrets(input);
        assertFalse(result.contains("secret123"));
        assertTrue(result.contains("********"));
    }

    @Test
    void maskSecrets_shouldMaskSecretInText() {
        String input = "Using client_secret: mysupersecret for OAuth";
        String result = SecretMaskingUtil.maskSecrets(input);
        assertFalse(result.contains("mysupersecret"));
        assertTrue(result.contains("********"));
    }

    @Test
    void maskSecrets_shouldMaskTokenInText() {
        String input = "Bearer token=abc123xyz";
        String result = SecretMaskingUtil.maskSecrets(input);
        assertFalse(result.contains("abc123xyz"));
        assertTrue(result.contains("********"));
    }

    // The bridge's payloads are JSON — the quoted-key form is the one that matters
    // in production. These pin the wire format the prose patterns can never match.

    @Test
    void maskSecrets_shouldMaskJsonQuotedPassword() {
        String result = SecretMaskingUtil.maskSecrets("{\"user\":\"bob\",\"password\":\"hunter2\"}");
        assertFalse(result.contains("hunter2"));
        assertEquals("{\"user\":\"bob\",\"password\":\"********\"}", result);
    }

    @Test
    void maskSecrets_shouldMaskJsonPasswordWithSpaces() {
        String result = SecretMaskingUtil.maskSecrets("{ \"password\" : \"hunter2\" }");
        assertFalse(result.contains("hunter2"));
    }

    @Test
    void maskSecrets_shouldMaskJsonTokenFields() {
        String result = SecretMaskingUtil.maskSecrets(
                "{\"accessToken\":\"eyJhbGciOiJSUzI1NiJ9.payload.sig\",\"refresh_token\":\"rt-123\"}");
        assertFalse(result.contains("eyJhbGciOiJSUzI1NiJ9"));
        assertFalse(result.contains("rt-123"));
    }

    @Test
    void maskSecrets_shouldMaskJsonSecretAndApiKey() {
        String result = SecretMaskingUtil.maskSecrets(
                "{\"clientSecret\":\"s3cr3t\",\"api_key\":\"ak-42\",\"apiKey\":\"ak-43\"}");
        assertFalse(result.contains("s3cr3t"));
        assertFalse(result.contains("ak-42"));
        assertFalse(result.contains("ak-43"));
    }

    @Test
    void maskSecrets_shouldMaskJsonUnquotedSecretValue() {
        String result = SecretMaskingUtil.maskSecrets("{\"apiKey\": 12345, \"count\": 7}");
        assertFalse(result.contains("12345"));
        assertTrue(result.contains("\"count\": 7"));
    }

    @Test
    void maskSecrets_shouldMaskAuthorizationBearerHeader() {
        String result = SecretMaskingUtil.maskSecrets(
                "Authorization: Bearer eyJhbGciOiJSUzI1NiJ9.abc.def");
        assertFalse(result.contains("eyJhbGciOiJSUzI1NiJ9"));
        assertTrue(result.contains("********"));
    }

    @Test
    void maskSecrets_shouldPreserveNonSecretJsonFields() {
        String input = "{\"transactionId\":\"tx-1\",\"planId\":\"p-9\",\"effectiveDate\":\"2026/01/01\"}";
        assertEquals(input, SecretMaskingUtil.maskSecrets(input));
    }

    @Test
    void maskSecrets_shouldHandleNullAndEmpty() {
        assertNull(SecretMaskingUtil.maskSecrets(null));
        assertEquals("", SecretMaskingUtil.maskSecrets(""));
    }

    @Test
    void maskSecrets_shouldPreserveNonSecretText() {
        String input = "Connecting to host=localhost port=1414";
        String result = SecretMaskingUtil.maskSecrets(input);
        assertEquals(input, result);
    }
}
