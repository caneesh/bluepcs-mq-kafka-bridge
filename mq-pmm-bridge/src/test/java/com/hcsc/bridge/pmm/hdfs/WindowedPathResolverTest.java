package com.hcsc.bridge.pmm.hdfs;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("WindowedPathResolver")
class WindowedPathResolverTest {

    private static final Clock FIXED = Clock.fixed(Instant.parse("2026-09-13T00:10:00Z"), ZoneOffset.UTC);

    private static WindowedPathResolver resolver(int hours, String zone) {
        return new WindowedPathResolver("/data/pmm/", "", hours, zone, "yyyy-MM-dd", ".xml", FIXED);
    }

    @Nested
    @DisplayName("4-hour windows in UTC")
    class FourHourUtc {

        private final WindowedPathResolver resolver = resolver(4, "UTC");

        @Test
        @DisplayName("maps the last millisecond of a window to that window")
        void lastMillisecond() {
            assertThat(resolver.windowLabel(Instant.parse("2026-09-13T03:59:59.999Z"))).isEqualTo("2026-09-13_00");
        }

        @Test
        @DisplayName("maps the first instant of a window to the new window")
        void firstInstant() {
            assertThat(resolver.windowLabel(Instant.parse("2026-09-13T04:00:00Z"))).isEqualTo("2026-09-13_04");
        }

        @Test
        @DisplayName("uses 20 for the last window of the day and rolls the date at midnight")
        void lastWindowAndMidnight() {
            assertThat(resolver.windowLabel(Instant.parse("2026-09-13T23:59:59Z"))).isEqualTo("2026-09-13_20");
            assertThat(resolver.windowLabel(Instant.parse("2026-09-14T00:00:00Z"))).isEqualTo("2026-09-14_00");
        }

        @Test
        @DisplayName("builds <base>/<date>_<HH>/<eventId>.xml with the trailing slash stripped")
        void resolvePath() {
            assertThat(resolver.resolve("abc123", Instant.parse("2026-09-13T09:15:00Z")))
                    .isEqualTo("/data/pmm/2026-09-13_08/abc123.xml");
            assertThat(resolver.windowDir(Instant.parse("2026-09-13T09:15:00Z")))
                    .isEqualTo("/data/pmm/2026-09-13_08");
        }

        @Test
        @DisplayName("quarantines flat under <base>/errors by default, or the configured error path")
        void quarantinePath() {
            assertThat(resolver.quarantinePath("abc")).isEqualTo("/data/pmm/errors/abc.xml");
            WindowedPathResolver custom = new WindowedPathResolver("/data/pmm", "/q/", 4, "UTC",
                    "yyyy-MM-dd", "xml", FIXED);
            assertThat(custom.quarantinePath("abc")).isEqualTo("/q/abc.xml");
            assertThat(custom.getExtension()).isEqualTo(".xml");
        }

        @Test
        @DisplayName("lists the current window and the previous ones across a date boundary")
        void recentWindowDirs() {
            assertThat(resolver.recentWindowDirs(2))
                    .containsExactly("/data/pmm/2026-09-13_00", "/data/pmm/2026-09-12_20");
            assertThat(resolver.recentWindowDirs(1)).containsExactly("/data/pmm/2026-09-13_00");
            assertThat(resolver.recentWindowDirs(0)).containsExactly("/data/pmm/2026-09-13_00");
        }
    }

    @Nested
    @DisplayName("other window sizes and zones")
    class Variants {

        @Test
        @DisplayName("supports 1, 6 and 12-hour windows")
        void otherSizes() {
            Instant t = Instant.parse("2026-09-13T13:30:00Z");
            assertThat(resolver(1, "UTC").windowLabel(t)).isEqualTo("2026-09-13_13");
            assertThat(resolver(6, "UTC").windowLabel(t)).isEqualTo("2026-09-13_12");
            assertThat(resolver(12, "UTC").windowLabel(t)).isEqualTo("2026-09-13_12");
            assertThat(resolver(24, "UTC").windowLabel(t)).isEqualTo("2026-09-13_00");
        }

        @Test
        @DisplayName("partitions by local time in a non-UTC zone")
        void localZone() {
            Instant t = Instant.parse("2026-09-13T03:30:00Z"); // 22:30 CDT the previous day
            assertThat(resolver(4, "America/Chicago").windowLabel(t)).isEqualTo("2026-09-12_20");
            assertThat(resolver(4, "UTC").windowLabel(t)).isEqualTo("2026-09-13_00");
        }

        @Test
        @DisplayName("is deterministic on DST transition days in a local zone")
        void dstDays() {
            WindowedPathResolver chicago = resolver(4, "America/Chicago");
            // 2026-03-08: clocks jump 02:00 -> 03:00 CST->CDT. 08:30Z = 03:30 CDT -> window 00
            assertThat(chicago.windowLabel(Instant.parse("2026-03-08T08:30:00Z"))).isEqualTo("2026-03-08_00");
            // 10:30Z = 05:30 CDT -> window 04
            assertThat(chicago.windowLabel(Instant.parse("2026-03-08T10:30:00Z"))).isEqualTo("2026-03-08_04");
            // 2026-11-01: 01:00-02:00 local happens twice. 06:30Z = 01:30 CDT, 07:30Z = 01:30 CST -> both window 00
            assertThat(chicago.windowLabel(Instant.parse("2026-11-01T06:30:00Z"))).isEqualTo("2026-11-01_00");
            assertThat(chicago.windowLabel(Instant.parse("2026-11-01T07:30:00Z"))).isEqualTo("2026-11-01_00");
            // and the same instant always maps to the same path
            assertThat(chicago.resolve("e", Instant.parse("2026-11-01T07:30:00Z")))
                    .isEqualTo(chicago.resolve("e", Instant.parse("2026-11-01T07:30:00Z")));
        }

        @Test
        @DisplayName("rejects a window size that does not divide 24, an invalid zone or a blank base path")
        void invalidConfig() {
            assertThatThrownBy(() -> resolver(5, "UTC")).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("window-hours");
            assertThatThrownBy(() -> resolver(0, "UTC")).isInstanceOf(IllegalStateException.class);
            assertThatThrownBy(() -> resolver(4, "Mars/Olympus")).isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("window-zone");
            assertThatThrownBy(() -> new WindowedPathResolver(" ", "", 4, "UTC", "yyyy-MM-dd", ".xml", FIXED))
                    .isInstanceOf(IllegalStateException.class).hasMessageContaining("base-path");
        }
    }
}
