package com.hcsc.bridge.diagnostics;

import org.springframework.beans.factory.ObjectProvider;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Proves the NameNode is reachable, resolving an HA nameservice through the Hadoop
 * configuration first: hdfs://PRDODPHA-style names are nameservices, not DNS hosts, so a
 * direct socket probe would fail against a perfectly healthy cluster.
 */
// Ordered so validate-only output reads in the sequence operators are used to:
// the message source first, then the destinations, then the credentials.
@Component
@Order(30)
public class HdfsConnectionCheck implements ReadinessCheck {

    private static final Logger logger = LoggerFactory.getLogger(HdfsConnectionCheck.class);

    @Value("${bridge.hdfs.namenode:}")
    private String hdfsNamenode;

    @Value("${bridge.hdfs.base-path:}")
    private String hdfsBasePath;

    @Value("${spring.profiles.active:}")
    private String activeProfile;

    private final ObjectProvider<org.apache.hadoop.conf.Configuration> hadoopConfigurationProvider;

    public HdfsConnectionCheck(ObjectProvider<org.apache.hadoop.conf.Configuration> hadoopConfigurationProvider) {
        // Absent in the local profile, where the check is skipped anyway.
        this.hadoopConfigurationProvider = hadoopConfigurationProvider;
    }


    @Override
    public String name() {
        return "HDFS_CONNECTION";
    }

    @Override
    public CheckResult run() {
        String name = name();

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
            // 8020 is the NameNode default the Hadoop client also assumes for port-less URIs
            int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 8020;

            // hdfs://PRDODPHA-style names are HA *nameservices* (dfs.nameservices), not
            // DNS hosts — a direct socket probe would UnknownHostException against a
            // perfectly healthy cluster. Resolve the real NameNode pair from the Hadoop
            // config (HADOOP_CONF_DIR) and probe those instead.
            if (!isDnsResolvable(host)) {
                return checkHaNameservice(name, host);
            }

            try (java.net.Socket socket = new java.net.Socket()) {
                socket.connect(new java.net.InetSocketAddress(host, port), 5000);
            }

            String message = String.format("HDFS namenode reachable at %s:%d", host, port);
            logger.info("[PASS] {}: {}", name, message);
            return CheckResult.pass(name, message);
        } catch (Exception e) {
            String message = String.format("Cannot reach HDFS namenode - %s", e.getMessage());
            logger.error("[FAIL] {}: {}", name, message);
            return CheckResult.fail(name, message);
        }
    }
    CheckResult checkHaNameservice(String name, String nameservice) {
        org.apache.hadoop.conf.Configuration conf =
                hadoopConfigurationProvider != null ? hadoopConfigurationProvider.getIfAvailable() : null;
        if (conf == null) {
            String message = String.format(
                    "%s is not DNS-resolvable and no Hadoop configuration is available "
                            + "to resolve it as an HA nameservice", nameservice);
            logger.error("[FAIL] {}: {}", name, message);
            return CheckResult.fail(name, message);
        }

        String nnIds = conf.get("dfs.ha.namenodes." + nameservice);
        if (nnIds == null || nnIds.trim().isEmpty()) {
            String message = String.format(
                    "%s is not DNS-resolvable and dfs.ha.namenodes.%s is not defined — "
                            + "is HADOOP_CONF_DIR pointing at the cluster's hdfs-site.xml?",
                    nameservice, nameservice);
            logger.error("[FAIL] {}: {}", name, message);
            return CheckResult.fail(name, message);
        }

        List<String> failures = new ArrayList<>();
        for (String id : nnIds.split(",")) {
            String nnId = id.trim();
            String rpcAddress = conf.get("dfs.namenode.rpc-address." + nameservice + "." + nnId);
            if (rpcAddress == null || rpcAddress.trim().isEmpty()) {
                failures.add(nnId + " (no rpc-address configured)");
                continue;
            }
            String[] hostPort = rpcAddress.split(":");
            int nnPort = hostPort.length > 1 ? Integer.parseInt(hostPort[1]) : 8020;
            try (java.net.Socket socket = new java.net.Socket()) {
                socket.connect(new java.net.InetSocketAddress(hostPort[0], nnPort), 5000);
                String message = String.format("HA nameservice %s: namenode %s (%s) reachable",
                        nameservice, nnId, rpcAddress);
                logger.info("[PASS] {}: {}", name, message);
                return CheckResult.pass(name, message);
            } catch (Exception e) {
                failures.add(rpcAddress + " (" + e.getMessage() + ")");
            }
        }

        String message = String.format("No namenode of HA nameservice %s reachable: %s",
                nameservice, failures);
        logger.error("[FAIL] {}: {}", name, message);
        return CheckResult.fail(name, message);
    }
    private boolean isDnsResolvable(String host) {
        try {
            java.net.InetAddress.getByName(host);
            return true;
        } catch (java.net.UnknownHostException e) {
            return false;
        }
    }
    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
