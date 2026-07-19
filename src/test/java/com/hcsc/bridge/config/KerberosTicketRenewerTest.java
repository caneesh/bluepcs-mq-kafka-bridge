package com.hcsc.bridge.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

@DisplayName("KerberosTicketRenewer")
class KerberosTicketRenewerTest {

    @Test
    @DisplayName("should invoke the relogin action on each renewal")
    void shouldInvokeReloginAction() {
        AtomicInteger invocations = new AtomicInteger();
        KerberosTicketRenewer renewer = new KerberosTicketRenewer(invocations::incrementAndGet);

        renewer.renewTicket();
        renewer.renewTicket();

        assertThat(invocations.get()).isEqualTo(2);
    }

    @Test
    @DisplayName("should never propagate relogin failures")
    void shouldSwallowReloginFailures() {
        KerberosTicketRenewer renewer = new KerberosTicketRenewer(() -> {
            throw new IOException("KDC unreachable");
        });

        // A thrown exception would kill the shared scheduler thread
        assertThatCode(renewer::renewTicket).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("should swallow runtime failures from the UGI layer too")
    void shouldSwallowRuntimeFailures() {
        KerberosTicketRenewer renewer = new KerberosTicketRenewer(() -> {
            throw new IllegalStateException("login user not initialized");
        });

        assertThatCode(renewer::renewTicket).doesNotThrowAnyException();
    }

    @Test
    @DisplayName("should stay wired to scheduling and the kerberos-enabled flag")
    void shouldKeepSchedulingAndConditionalAnnotations() throws NoSuchMethodException {
        Method renewTicket = KerberosTicketRenewer.class.getMethod("renewTicket");
        Scheduled scheduled = renewTicket.getAnnotation(Scheduled.class);
        assertThat(scheduled).isNotNull();
        assertThat(scheduled.fixedDelayString()).contains("bridge.hdfs.kerberos.relogin-interval-ms");

        ConditionalOnProperty conditional =
                KerberosTicketRenewer.class.getAnnotation(ConditionalOnProperty.class);
        assertThat(conditional).isNotNull();
        assertThat(conditional.name()).contains("bridge.hdfs.kerberos.enabled");
        assertThat(conditional.havingValue()).isEqualTo("true");
    }
}
