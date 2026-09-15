package com.hcsc.bridge.orchestrator;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("ProcessingResult: the acknowledgement rule")
class ProcessingResultTest {

    @Nested
    @DisplayName("what may be acknowledged")
    class Acknowledgeable {

        @Test
        @DisplayName("terminal outcomes may be acknowledged; a failure may not")
        void rule() {
            assertThat(ProcessingResult.success("e", "/p", "7").isAcknowledgeable()).isTrue();
            assertThat(ProcessingResult.success("e", "/p").isAcknowledgeable()).isTrue();
            assertThat(ProcessingResult.quarantined("e", "/errors/e.json", "PARSE_ERROR", "bad")
                    .isAcknowledgeable()).isTrue();
            assertThat(ProcessingResult.discarded("e", "/errors/e.json", "POISON", "gave up")
                    .isAcknowledgeable()).isTrue();
            assertThat(ProcessingResult.discardedWithoutPayload("e", "body unreadable", "POISON", "gave up")
                    .isAcknowledgeable()).isTrue();
            assertThat(ProcessingResult.failure("e", "HDFS_ERROR", "namenode down")
                    .isAcknowledgeable()).isFalse();
        }
    }

    @Nested
    @DisplayName("evidence is required, not assumed")
    class EvidenceRequired {

        @Test
        @DisplayName("a quarantine without a durable path cannot be constructed")
        void quarantineNeedsAPath() {
            assertThatThrownBy(() -> ProcessingResult.quarantined("e", null, "PARSE_ERROR", "bad"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("stay on the queue");
            assertThatThrownBy(() -> ProcessingResult.quarantined("e", "  ", "PARSE_ERROR", "bad"))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("a discard without a durable path cannot be constructed")
        void discardNeedsAPath() {
            assertThatThrownBy(() -> ProcessingResult.discarded("e", null, "POISON", "gave up"))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("a success must name where the payload landed")
        void successNeedsAPath() {
            assertThatThrownBy(() -> ProcessingResult.success("e", null, "7"))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> ProcessingResult.success("e", null))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("acknowledging with no copy at all requires a stated reason")
        void payloadlessDiscardNeedsAReason() {
            assertThatThrownBy(() -> ProcessingResult.discardedWithoutPayload("e", "", "POISON", "gave up"))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("explained");
            ProcessingResult r = ProcessingResult.discardedWithoutPayload(
                    "e", "body unreadable", "POISON", "gave up");
            assertThat(r.getHdfsPath()).isNull();
            assertThat(r.getNoEvidenceReason()).isEqualTo("body unreadable");
            assertThat(r.isDiscarded()).isTrue();
        }

        @Test
        @DisplayName("a failure carries no path and is never acknowledgeable")
        void failureCarriesNoEvidence() {
            ProcessingResult r = ProcessingResult.failure("e", "API_ERROR", "503");
            assertThat(r.getHdfsPath()).isNull();
            assertThat(r.isFailed()).isTrue();
            assertThat(r.isAcknowledgeable()).isFalse();
        }
    }
}
