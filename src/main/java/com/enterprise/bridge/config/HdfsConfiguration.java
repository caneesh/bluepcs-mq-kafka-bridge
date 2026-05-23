package com.enterprise.bridge.config;

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

        if (kerberosEnabled) {
            configuration.set("hadoop.security.authentication", "kerberos");
            configuration.set("hadoop.security.authorization", "true");
            UserGroupInformation.setConfiguration(configuration);
            UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytab);
            logger.info("Logged in to Kerberos as: {}", kerberosPrincipal);
        }

        logger.info("Initialized Hadoop configuration for namenode: {}", namenode);
        return configuration;
    }

    public String getBasePath() {
        return basePath;
    }
}
