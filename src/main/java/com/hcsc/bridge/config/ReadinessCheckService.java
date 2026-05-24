package com.hcsc.bridge.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.util.ArrayList;
import java.util.List;

@Service
public class ReadinessCheckService {

    private static final Logger logger = LoggerFactory.getLogger(ReadinessCheckService.class);

    @Value("${bridge.mq.host:}")
    private String mqHost;

    @Value("${bridge.mq.port:1414}")
    private int mqPort;

    @Value("${bridge.kafka.bootstrap-servers:}")
    private String kafkaBootstrapServers;

    @Value("${bridge.hdfs.namenode:}")
    private String hdfsNamenode;

    @Value("${bridge.hdfs.base-path:}")
    private String hdfsBasePath;

    @Value("${bridge.security.token-url:}")
    private String oauthTokenUrl;

    @Value("${bridge.security.client-id:}")
    private String oauthClientId;

    @Value("${bridge.security.client-secret:}")
    private String oauthClientSecret;

    @Value("${spring.profiles.active:}")
    private String activeProfile;

    private final RestTemplate restTemplate;

    public ReadinessCheckService() {
        this.restTemplate = new RestTemplate();
    }

    public ReadinessReport runAllChecks() {
        logger.info("=== RUNNING READINESS CHECKS ===");

        List<CheckResult> results = new ArrayList<>();

        results.add(checkMqConnection());
        results.add(checkKafkaConnection());
        results.add(checkHdfsConnection());
        results.add(checkOAuthToken());

        ReadinessReport report = new ReadinessReport(results);

        logger.info("=== VALIDATION RESULT: {} ===", report.isPassed() ? "PASSED" : "FAILED");

        return report;
    }

    private CheckResult checkMqConnection() {
        String name = "MQ_CONNECTION";
        logger.info("Checking {}...", name);

        if (isBlank(mqHost)) {
            return CheckResult.skip(name, "MQ host not configured");
        }

        try {
            java.net.Socket socket = new java.net.Socket();
            socket.connect(new java.net.InetSocketAddress(mqHost, mqPort), 5000);
            socket.close();
            String message = String.format("MQ reachable at %s:%d", mqHost, mqPort);
            logger.info("[PASS] {}: {}", name, message);
            return CheckResult.pass(name, message);
        } catch (Exception e) {
            String message = String.format("Cannot reach MQ at %s:%d - %s", mqHost, mqPort, e.getMessage());
            logger.error("[FAIL] {}: {}", name, message);
            return CheckResult.fail(name, message);
        }
    }

    private CheckResult checkKafkaConnection() {
        String name = "KAFKA_CONNECTION";
        logger.info("Checking {}...", name);

        if (isBlank(kafkaBootstrapServers)) {
            return CheckResult.skip(name, "Kafka bootstrap servers not configured");
        }

        try {
            String[] servers = kafkaBootstrapServers.split(",");
            for (String server : servers) {
                String[] parts = server.trim().split(":");
                String host = parts[0];
                int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 9092;

                java.net.Socket socket = new java.net.Socket();
                socket.connect(new java.net.InetSocketAddress(host, port), 5000);
                socket.close();

                String message = String.format("Kafka reachable at %s:%d", host, port);
                logger.info("[PASS] {}: {}", name, message);
                return CheckResult.pass(name, message);
            }
            return CheckResult.fail(name, "No Kafka brokers reachable");
        } catch (Exception e) {
            String message = String.format("Cannot reach Kafka - %s", e.getMessage());
            logger.error("[FAIL] {}: {}", name, message);
            return CheckResult.fail(name, message);
        }
    }

    private CheckResult checkHdfsConnection() {
        String name = "HDFS_CONNECTION";
        logger.info("Checking {}...", name);

        if (isBlank(hdfsNamenode)) {
            return CheckResult.skip(name, "HDFS namenode not configured");
        }

        if ("local".equals(activeProfile)) {
            return CheckResult.skip(name, "Skipped in local profile");
        }

        try {
            String namenodeUrl = hdfsNamenode;
            if (namenodeUrl.startsWith("hdfs://")) {
                namenodeUrl = namenodeUrl.substring(7);
            }
            String[] parts = namenodeUrl.split(":");
            String host = parts[0];
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 8020;

            java.net.Socket socket = new java.net.Socket();
            socket.connect(new java.net.InetSocketAddress(host, port), 5000);
            socket.close();

            String message = String.format("HDFS namenode reachable at %s:%d", host, port);
            logger.info("[PASS] {}: {}", name, message);
            return CheckResult.pass(name, message);
        } catch (Exception e) {
            String message = String.format("Cannot reach HDFS namenode - %s", e.getMessage());
            logger.error("[FAIL] {}: {}", name, message);
            return CheckResult.fail(name, message);
        }
    }

    private CheckResult checkOAuthToken() {
        String name = "OAUTH_TOKEN";
        logger.info("Checking {}...", name);

        if (isBlank(oauthTokenUrl) || isBlank(oauthClientId) || isBlank(oauthClientSecret)) {
            return CheckResult.skip(name, "OAuth not fully configured");
        }

        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);

            MultiValueMap<String, String> body = new LinkedMultiValueMap<>();
            body.add("grant_type", "client_credentials");
            body.add("client_id", oauthClientId);
            body.add("client_secret", oauthClientSecret);

            HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(body, headers);
            ResponseEntity<String> response = restTemplate.postForEntity(oauthTokenUrl, request, String.class);

            if (response.getStatusCode().is2xxSuccessful()) {
                String message = "OAuth token acquired successfully";
                logger.info("[PASS] {}: {}", name, message);
                return CheckResult.pass(name, message);
            } else {
                String message = String.format("OAuth token request failed with status %s", response.getStatusCode());
                logger.error("[FAIL] {}: {}", name, message);
                return CheckResult.fail(name, message);
            }
        } catch (Exception e) {
            String message = String.format("OAuth token acquisition failed - %s", e.getMessage());
            logger.error("[FAIL] {}: {}", name, message);
            return CheckResult.fail(name, message);
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    public static class CheckResult {
        private final String name;
        private final Status status;
        private final String message;

        public enum Status {
            PASS, FAIL, SKIP
        }

        private CheckResult(String name, Status status, String message) {
            this.name = name;
            this.status = status;
            this.message = message;
        }

        public static CheckResult pass(String name, String message) {
            return new CheckResult(name, Status.PASS, message);
        }

        public static CheckResult fail(String name, String message) {
            return new CheckResult(name, Status.FAIL, message);
        }

        public static CheckResult skip(String name, String message) {
            return new CheckResult(name, Status.SKIP, message);
        }

        public String getName() {
            return name;
        }

        public Status getStatus() {
            return status;
        }

        public String getMessage() {
            return message;
        }

        public boolean isPassed() {
            return status == Status.PASS;
        }

        public boolean isFailed() {
            return status == Status.FAIL;
        }

        public boolean isSkipped() {
            return status == Status.SKIP;
        }
    }

    public static class ReadinessReport {
        private final List<CheckResult> results;

        public ReadinessReport(List<CheckResult> results) {
            this.results = new ArrayList<>(results);
        }

        public List<CheckResult> getResults() {
            return results;
        }

        public boolean isPassed() {
            return results.stream().noneMatch(CheckResult::isFailed);
        }

        public long getPassedCount() {
            return results.stream().filter(CheckResult::isPassed).count();
        }

        public long getFailedCount() {
            return results.stream().filter(CheckResult::isFailed).count();
        }

        public long getSkippedCount() {
            return results.stream().filter(CheckResult::isSkipped).count();
        }
    }
}
