package com.hcsc.bridge.config;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * Keeps the Kerberos TGT fresh for 24/7 operation. The keytab login happens once at
 * startup ({@link HdfsConfiguration}); without periodic renewal the ticket lapses at
 * its lifetime boundary and HDFS writes start failing with auth errors until the
 * process is restarted. {@code checkTGTAndReloginFromKeytab} is a cheap no-op while
 * the ticket is fresh and relogins from the keytab as it ages, so a short interval
 * is safe.
 */
@Component
@Profile("!local")
@ConditionalOnProperty(name = "bridge.hdfs.kerberos.enabled", havingValue = "true")
public class KerberosTicketRenewer {

    private static final Logger logger = LoggerFactory.getLogger(KerberosTicketRenewer.class);

    /** Seam for tests: production wires this to the static UGI call. */
    @FunctionalInterface
    interface ReloginAction {
        void relogin() throws IOException;
    }

    private final ReloginAction reloginAction;

    @Autowired
    public KerberosTicketRenewer() {
        this(() -> UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab());
    }

    KerberosTicketRenewer(ReloginAction reloginAction) {
        this.reloginAction = reloginAction;
    }

    @Scheduled(fixedDelayString = "${bridge.hdfs.kerberos.relogin-interval-ms:300000}")
    public void renewTicket() {
        try {
            reloginAction.relogin();
            logger.debug("Kerberos TGT check/relogin completed");
        } catch (Exception e) {
            // Never propagate: a failed renewal must not kill the scheduler thread.
            // The next scheduled attempt retries; persistent failures surface as HDFS
            // auth errors and the health watchdog/monitoring take over from there.
            logger.warn("Kerberos TGT relogin failed (will retry on next interval): {}", e.getMessage());
        }
    }
}
