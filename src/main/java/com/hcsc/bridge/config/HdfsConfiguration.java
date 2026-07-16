package com.hcsc.bridge.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Profile;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;

@org.springframework.context.annotation.Configuration
@Profile("!local")
public class HdfsConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(HdfsConfiguration.class);

    @Value("${bridge.hdfs.namenode:hdfs://localhost:9000}")
    private String namenode;

    @Value("${bridge.hdfs.replication:3}")
    private int replication;

    @Value("${bridge.hdfs.base-path:}")
    private String basePath;

    @Value("${bridge.hdfs.config-dir:${HADOOP_CONF_DIR:}}")
    private String configDir;

    @Value("${bridge.hdfs.kerberos.enabled:false}")
    private boolean kerberosEnabled;

    @Value("${bridge.hdfs.kerberos.principal:}")
    private String kerberosPrincipal;

    @Value("${bridge.hdfs.kerberos.keytab:}")
    private String kerberosKeytab;

    @Value("${bridge.hdfs.kerberos.namenode-principal:}")
    private String namenodePrincipal;

    @Value("${bridge.hdfs.kerberos.resourcemanager-principal:}")
    private String resourcemanagerPrincipal;

    @PostConstruct
    public void validate() {
        if (basePath == null || basePath.isEmpty()) {
            throw new IllegalStateException("bridge.hdfs.base-path must be configured");
        }
        logger.info("HDFS base path: {}", basePath);

        if (kerberosEnabled) {
            if (kerberosPrincipal == null || kerberosPrincipal.isEmpty()) {
                throw new IllegalStateException("Kerberos principal required when Kerberos is enabled");
            }
            if (kerberosKeytab == null || kerberosKeytab.isEmpty()) {
                throw new IllegalStateException("Kerberos keytab required when Kerberos is enabled");
            }
            logger.info("Kerberos authentication enabled for principal: {}", kerberosPrincipal);
        }
    }

    @Bean
    public Configuration hadoopConfiguration() throws IOException {
        Configuration configuration = new Configuration();

        if (configDir != null && !configDir.isEmpty()) {
            File confDir = new File(configDir);
            if (confDir.exists() && confDir.isDirectory()) {
                File coreSite = new File(confDir, "core-site.xml");
                File hdfsSite = new File(confDir, "hdfs-site.xml");
                if (coreSite.exists()) {
                    configuration.addResource(coreSite.toURI().toURL());
                    logger.info("Loaded core-site.xml from {}", coreSite.getAbsolutePath());
                }
                if (hdfsSite.exists()) {
                    configuration.addResource(hdfsSite.toURI().toURL());
                    logger.info("Loaded hdfs-site.xml from {}", hdfsSite.getAbsolutePath());
                }
            } else {
                logger.warn("HADOOP_CONF_DIR not found or not a directory: {}", configDir);
            }
        }

        configuration.set("fs.defaultFS", namenode);
        configuration.setInt("dfs.replication", replication);
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        validateNameserviceResolvable(configuration);

        if (kerberosEnabled) {
            configuration.set("hadoop.security.authentication", "kerberos");
            configuration.set("hadoop.security.authorization", "true");

            if (namenodePrincipal != null && !namenodePrincipal.isEmpty()) {
                configuration.set("dfs.namenode.kerberos.principal", namenodePrincipal);
            }
            if (resourcemanagerPrincipal != null && !resourcemanagerPrincipal.isEmpty()) {
                configuration.set("yarn.resourcemanager.principal", resourcemanagerPrincipal);
            }

            UserGroupInformation.setConfiguration(configuration);
            UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytab);
            logger.info("Logged in to Kerberos as: {}", kerberosPrincipal);
        }

        logger.info("Initialized Hadoop configuration for namenode: {}", namenode);
        return configuration;
    }

    /**
     * A namenode URI without a port (e.g. hdfs://TSTODPHA, hdfs://PRDODPHA) is an HA
     * nameservice logical name, not a hostname: the Hadoop client resolves it through
     * the dfs.nameservices / dfs.ha.namenodes.* settings in hdfs-site.xml. Fail fast
     * with a clear message when those settings are absent, instead of the obscure
     * UnknownHostException the client would otherwise throw on first use.
     */
    private void validateNameserviceResolvable(Configuration configuration) {
        java.net.URI uri = java.net.URI.create(namenode);
        boolean looksLikeNameservice = uri.getPort() == -1;
        if (!looksLikeNameservice) {
            return;
        }

        String nameservices = configuration.get("dfs.nameservices", "");
        String host = uri.getHost() != null ? uri.getHost() : uri.getAuthority();
        if (!nameservices.contains(host)) {
            throw new IllegalStateException(String.format(
                    "Namenode '%s' is an HA nameservice logical name, but '%s' is not defined in "
                            + "dfs.nameservices (found: '%s'). Set HADOOP_CONF_DIR (or "
                            + "bridge.hdfs.config-dir) to a directory containing the cluster's "
                            + "core-site.xml and hdfs-site.xml with the HA settings.",
                    namenode, host, nameservices.isEmpty() ? "<none>" : nameservices));
        }
        logger.info("HA nameservice '{}' resolved via hdfs-site.xml (dfs.nameservices={})", host, nameservices);
    }

    public String getBasePath() {
        return basePath;
    }
}
