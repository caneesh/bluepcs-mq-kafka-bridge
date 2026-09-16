package com.hcsc.bridge.pmm.hdfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.time.Clock;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

/**
 * Maps a message to its landing file in the time-partitioned tree
 * {@code <base-path>/<date>_<HH>/<eventId><ext>}, where {@code HH} is the start hour of
 * the enclosing window (every {@code window-hours} from midnight in {@code window-zone}:
 * 00, 04, 08, 12, 16, 20 for the 4-hour default).
 *
 * <p>One directory level per window, named {@code 2026-09-16_04}, rather than a date
 * directory containing hour directories: the downstream reader takes one window at a time,
 * and the retention sweep moves and expires whole windows.
 *
 * <p>The anchor instant passed by the caller must be stable across redeliveries (the
 * broker's JMSTimestamp, not the receive time): the writer's idempotency check looks
 * for the target path, so a redelivery that computed a different window would write a
 * second copy.
 */
@Component
public class WindowedPathResolver {

    private static final Logger logger = LoggerFactory.getLogger(WindowedPathResolver.class);

    /**
     * Joins the date and the window's start hour into ONE directory name. Part of the
     * downstream contract: readers list windows, and scripts/pmm-hdfs-cleanup.sh matches
     * directories on {@code yyyy-MM-dd_HH}.
     */
    private static final String WINDOW_SEPARATOR = "_";

    private final String basePath;
    private final String errorPath;
    private final int windowHours;
    private final ZoneId zone;
    private final DateTimeFormatter dateFormatter;
    private final String extension;
    private final Clock clock;

    public WindowedPathResolver(
            @Value("${bridge.hdfs.base-path}") String basePath,
            @Value("${bridge.hdfs.error-path:}") String errorPath,
            @Value("${bridge.pmm.hdfs.window-hours:4}") int windowHours,
            @Value("${bridge.pmm.hdfs.window-zone:UTC}") String zone,
            @Value("${bridge.pmm.hdfs.date-pattern:yyyy-MM-dd}") String datePattern,
            @Value("${bridge.pmm.hdfs.file-extension:.xml}") String extension,
            Clock clock) {
        if (basePath == null || basePath.trim().isEmpty()) {
            throw new IllegalStateException("bridge.hdfs.base-path is required");
        }
        if (windowHours < 1 || windowHours > 24 || 24 % windowHours != 0) {
            throw new IllegalStateException("bridge.pmm.hdfs.window-hours must divide 24 evenly (1,2,3,4,6,8,12,24), got "
                    + windowHours);
        }
        this.basePath = basePath.trim().replaceAll("/+$", "");
        this.errorPath = (errorPath == null || errorPath.trim().isEmpty())
                ? this.basePath + "/errors"
                : errorPath.trim().replaceAll("/+$", "");
        this.windowHours = windowHours;
        try {
            this.zone = ZoneId.of(zone == null || zone.trim().isEmpty() ? "UTC" : zone.trim());
        } catch (DateTimeException e) {
            throw new IllegalStateException("bridge.pmm.hdfs.window-zone is not a valid zone id: " + zone, e);
        }
        try {
            this.dateFormatter = DateTimeFormatter.ofPattern(
                    datePattern == null || datePattern.trim().isEmpty() ? "yyyy-MM-dd" : datePattern.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException("bridge.pmm.hdfs.date-pattern is invalid: " + datePattern, e);
        }
        String ext = extension == null ? "" : extension.trim();
        this.extension = ext.isEmpty() || ext.startsWith(".") ? ext : "." + ext;
        this.clock = clock;
        logger.info("PMM landing tree: {}/<{}>_<HH>/<eventId>{} with {}-hour windows in {}; quarantine {}",
                this.basePath, this.dateFormatter.toString().isEmpty() ? datePattern : datePattern,
                this.extension, windowHours, this.zone, this.errorPath);
    }

    /** Target file for a message anchored at {@code anchor}: {@code <base>/<date>_<HH>/<eventId><ext>}. */
    public String resolve(String eventId, Instant anchor) {
        return windowDir(anchor) + "/" + eventId + extension;
    }

    /** Directory of the window containing {@code anchor}: {@code <base>/<date>_<HH>}. */
    public String windowDir(Instant anchor) {
        return basePath + "/" + windowLabel(anchor);
    }

    /** {@code <date>_<HH>}: the window's directory name, used in audit metadata and logs. */
    public String windowLabel(Instant anchor) {
        ZonedDateTime start = windowStart(anchor);
        return dateFormatter.format(start) + WINDOW_SEPARATOR + String.format("%02d", start.getHour());
    }

    /** Quarantine file for a message: {@code <error-path>/<eventId><ext>} (flat, unwindowed). */
    public String quarantinePath(String eventId) {
        return errorPath + "/" + eventId + extension;
    }

    /**
     * The current window's directory and the {@code count - 1} preceding ones, newest
     * first — the bounded set the backlog monitor scans.
     */
    public List<String> recentWindowDirs(int count) {
        List<String> dirs = new ArrayList<>();
        ZonedDateTime cursor = windowStart(clock.instant());
        for (int i = 0; i < Math.max(1, count); i++) {
            String dir = basePath + "/" + dateFormatter.format(cursor)
                    + WINDOW_SEPARATOR + String.format("%02d", cursor.getHour());
            if (!dirs.contains(dir)) {
                dirs.add(dir);
            }
            cursor = windowStart(cursor.toInstant().minus(windowHours, ChronoUnit.HOURS));
        }
        return dirs;
    }

    public String getBasePath() {
        return basePath;
    }

    public String getErrorPath() {
        return errorPath;
    }

    public String getExtension() {
        return extension;
    }

    public int getWindowHours() {
        return windowHours;
    }

    public ZoneId getZone() {
        return zone;
    }

    private ZonedDateTime windowStart(Instant anchor) {
        ZonedDateTime zoned = anchor.atZone(zone);
        int startHour = (zoned.getHour() / windowHours) * windowHours;
        return zoned.truncatedTo(ChronoUnit.HOURS).withHour(startHour);
    }
}
