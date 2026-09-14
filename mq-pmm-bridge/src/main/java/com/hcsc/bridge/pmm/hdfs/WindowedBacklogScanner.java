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
 * Backlog candidates for the monitor: files in the current window directory and the
 * preceding {@code bridge.pmm.monitor.backlog-windows - 1} windows. Bounded on purpose —
 * the tree grows by six directories a day and must never be walked recursively on a
 * monitoring cycle. A missing window directory (no traffic yet) contributes nothing.
 */
@Component
public class WindowedBacklogScanner implements BacklogScanner {

    private final HdfsFileOperations hdfsFileOperations;
    private final WindowedPathResolver resolver;
    private final int backlogWindows;

    public WindowedBacklogScanner(HdfsFileOperations hdfsFileOperations,
                                  WindowedPathResolver resolver,
                                  @Value("${bridge.pmm.monitor.backlog-windows:2}") int backlogWindows) {
        this.hdfsFileOperations = hdfsFileOperations;
        this.resolver = resolver;
        this.backlogWindows = Math.max(1, backlogWindows);
    }

    @Override
    public List<HdfsFileInfo> scan() throws IOException {
        List<HdfsFileInfo> files = new ArrayList<>();
        for (String dir : resolver.recentWindowDirs(backlogWindows)) {
            files.addAll(hdfsFileOperations.listFiles(dir));
        }
        return files;
    }
}
