package com.hcsc.bridge.pmm.config;

import com.hcsc.bridge.diagnostics.CheckResult;
import com.hcsc.bridge.pmm.template.PmmRequestTemplate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.net.ServerSocket;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@DisplayName("the readiness checks the PMM bridge contributes")
class PmmReadinessChecksTest {

    @SuppressWarnings("unchecked")
    private static ObjectProvider<PmmRequestTemplate> provider(PmmRequestTemplate template) {
        ObjectProvider<PmmRequestTemplate> p = mock(ObjectProvider.class);
        when(p.getIfAvailable()).thenReturn(template);
        return p;
    }

    @Nested
    @DisplayName("PMM_API_REACHABLE")
    class ApiReachable {

        @Test
        @DisplayName("passes when the web-service host accepts a connection")
        void passesWhenReachable() throws Exception {
            try (ServerSocket listening = new ServerSocket(0)) {
                CheckResult result = new PmmApiReachableCheck(
                        "http://localhost:" + listening.getLocalPort() + "/pmm").run();

                assertThat(result.isPassed()).isTrue();
                assertThat(result.getName()).isEqualTo("PMM_API_REACHABLE");
            }
        }

        @Test
        @DisplayName("fails when the host is unreachable or the URL has no host")
        void failsWhenUnreachable() {
            assertThat(new PmmApiReachableCheck("http://localhost:1/pmm").run().isFailed()).isTrue();
            assertThat(new PmmApiReachableCheck("not-a-url").run().isFailed()).isTrue();
        }

        @Test
        @DisplayName("skips when the URL is not configured")
        void skipsWhenUnconfigured() {
            assertThat(new PmmApiReachableCheck("").run().isSkipped()).isTrue();
        }
    }

    @Nested
    @DisplayName("PMM_TEMPLATE")
    class Template {

        @Test
        @DisplayName("passes when the template renders with no placeholder left behind")
        void passesWhenTemplateRenders() {
            PmmRequestTemplate template = new PmmRequestTemplate("<r>${value1}|${value2}</r>", "inline", true);

            CheckResult result = new PmmTemplateCheck(provider(template)).run();

            assertThat(result.isPassed()).isTrue();
            assertThat(result.getMessage()).contains("inline");
        }

        @Test
        @DisplayName("fails when the template bean never loaded")
        void failsWithoutTemplate() {
            CheckResult result = new PmmTemplateCheck(provider(null)).run();

            assertThat(result.isFailed()).isTrue();
            assertThat(result.getMessage()).contains("not available");
        }

        @Test
        @DisplayName("fails when a placeholder survives rendering")
        void failsWhenPlaceholderSurvives() {
            // A template whose only placeholder is unknown cannot be built at all, so the
            // surviving-placeholder guard is exercised through a partial template that keeps
            // a literal dollar-brace token the renderer does not substitute.
            PmmRequestTemplate template = new PmmRequestTemplate(
                    "<r>${value1}${value2}</r>", "inline", true) {
                @Override
                public String render(com.hcsc.bridge.pmm.xml.PmmExtractedValues values) {
                    return "<r>${value1}</r>";
                }
            };

            assertThat(new PmmTemplateCheck(provider(template)).run().isFailed()).isTrue();
        }
    }
}
