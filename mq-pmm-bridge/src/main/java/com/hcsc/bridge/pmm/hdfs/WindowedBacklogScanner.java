package com.hcsc.bridge.pmm.hdfs;

import com.hcsc.bridge.config.BacklogScanner;
import com.hcsc.bridge.hdfs.HdfsFileOperations;
import com.hcsc.bridge.hdfs.HdfsFileOperations.HdfsFileInfo;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Backlog candidates for the monitor. PMM files are read IN PLACE by the downstream
 * job (nothing moves them out of the window directory), so an old landed {@code .xml}
 * is normal, not a backlog. What does indicate trouble is an in-flight temp file
 * ({@code <eventId>.<uuid>.xml.tmp}) that outlives the monitor's age threshold: a
 * crashed or wedged safe-write. Only those are reported, from the current window and
 * the preceding {@code bridge.pmm.monitor.backlog-windows - 1} windows — bounded on
 * purpose, never a recursive walk of a tree that grows six directories a day.
 */
@Component
public class WindowedBacklogScanner implements BacklogScanner {

    private final HdfsFileOperations hdfsFileOperations;
    private final WindowedPathResolver resolver;
    private final int backlogWindows;
    private final String tempFileSuffix;

    public WindowedBacklogScanner(HdfsFileOperations hdfsFileOperations,
                                  WindowedPathResolver resolver,
                                  @Value("${bridge.pmm.monitor.backlog-windows:2}") int backlogWindows,
                                  @Value("${bridge.hdfs.temp-suffix:.tmp}") String tempSuffix) {
        this.hdfsFileOperations = hdfsFileOperations;
        this.resolver = resolver;
        this.backlogWindows = Math.max(1, backlogWindows);
        this.tempFileSuffix = resolver.getExtension() + tempSuffix;
    }

    @Override
    public List<HdfsFileInfo> scan() throws IOException {
        List<HdfsFileInfo> inFlight = new ArrayList<>();
        for (String dir : resolver.recentWindowDirs(backlogWindows)) {
            for (HdfsFileInfo file : hdfsFileOperations.listFiles(dir)) {
                if (file.getPath().endsWith(tempFileSuffix)) {
                    inFlight.add(file);
                }
            }
        }
        return inFlight;
    }
}
