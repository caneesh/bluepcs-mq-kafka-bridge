package com.hcsc.bridge.diagnostics.startup;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.List;

/**
 * The HDFS and Kerberos rules both bridges enforce at startup. The keytab must exist AND
 * be readable by the service account: a keytab the process cannot read fails much later, as
 * an authentication error on the first write.
 */
public final class HdfsStartupRules {

    private static final Logger logger = LoggerFactory.getLogger(HdfsStartupRules.class);

    private final String hdfsNamenode;
    private final String hdfsBasePath;
    private final boolean hdfsKerberosEnabled;
    private final String hdfsKerberosPrincipal;
    private final String hdfsKerberosKeytab;

    public HdfsStartupRules(String hdfsNamenode, String hdfsBasePath, boolean hdfsKerberosEnabled, String hdfsKerberosPrincipal, String hdfsKerberosKeytab) {
        this.hdfsNamenode = hdfsNamenode;
        this.hdfsBasePath = hdfsBasePath;
        this.hdfsKerberosEnabled = hdfsKerberosEnabled;
        this.hdfsKerberosPrincipal = hdfsKerberosPrincipal;
        this.hdfsKerberosKeytab = hdfsKerberosKeytab;
    }

    public void validateHdfsConfig(List<String> errors, List<String> warnings) {
        logger.info("Validating HDFS configuration...");

        if (isBlank(hdfsNamenode)) {
            errors.add("[HDFS] bridge.hdfs.namenode is required");
        } else {
            logger.info("[HDFS] Namenode: {}", hdfsNamenode);
        }

        if (isBlank(hdfsBasePath)) {
            errors.add("[HDFS] bridge.hdfs.base-path is required");
        } else {
            logger.info("[HDFS] Base Path: {}", hdfsBasePath);
        }

        if (hdfsKerberosEnabled) {
            logger.info("[HDFS] Kerberos: ENABLED");

            if (isBlank(hdfsKerberosPrincipal)) {
                errors.add("[HDFS] bridge.hdfs.kerberos.principal is required when Kerberos is enabled");
            } else {
                logger.info("[HDFS] Kerberos Principal: {}", hdfsKerberosPrincipal);
            }

            if (isBlank(hdfsKerberosKeytab)) {
                errors.add("[HDFS] bridge.hdfs.kerberos.keytab is required when Kerberos is enabled");
            } else {
                File keytabFile = new File(hdfsKerberosKeytab);
                if (!keytabFile.exists()) {
                    errors.add("[HDFS] Keytab file not found: " + hdfsKerberosKeytab);
                } else if (!keytabFile.canRead()) {
                    errors.add("[HDFS] Keytab file not readable: " + hdfsKerberosKeytab);
                } else {
                    logger.info("[HDFS] Keytab: {} (exists, readable)", hdfsKerberosKeytab);
                }
            }
        } else {
            logger.info("[HDFS] Kerberos: DISABLED");
        }
    }
    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
