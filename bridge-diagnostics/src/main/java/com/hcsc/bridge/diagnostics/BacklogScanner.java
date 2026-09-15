package com.hcsc.bridge.diagnostics;

import com.hcsc.bridge.hdfs.HdfsFileOperations.HdfsFileInfo;

import java.io.IOException;
import java.util.List;

/**
 * Optional hook for {@link MonitorRunner}'s backlog check: returns the landing files
 * whose age should be evaluated. When no bean of this type exists the monitor lists
 * the flat {@code bridge.hdfs.base-path} directory, which is the PMM+ layout; an
 * application with a partitioned landing tree supplies a bounded scan of the
 * directories that can still hold unprocessed files.
 */
@FunctionalInterface
public interface BacklogScanner {

    List<HdfsFileInfo> scan() throws IOException;
}
